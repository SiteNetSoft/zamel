const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;
const Processor = @import("processor.zig").Processor;

/// A JSON path expression for extracting/setting values in JSON bodies.
/// Supports: $.key, $.key.nested, $.array[N], $.array[N].key
pub const JsonPath = struct {
    segments_buf: [16]Segment = undefined,
    len: usize = 0,

    pub const Segment = union(enum) {
        key: []const u8,
        index: usize,
    };

    pub fn segments(self: *const JsonPath) []const Segment {
        return self.segments_buf[0..self.len];
    }
};

/// Parse a JSON path expression like "$.name", "$.items[0].price", "$.data.nested"
pub fn parsePath(expr: []const u8) !JsonPath {
    if (expr.len < 2 or expr[0] != '$') return error.InvalidJsonPath;

    var result: JsonPath = .{};
    var i: usize = 1; // skip '$'

    while (i < expr.len) {
        if (expr[i] == '.') {
            i += 1;
            if (i >= expr.len) return error.InvalidJsonPath;
            // Read key until next . or [ or end
            const start = i;
            while (i < expr.len and expr[i] != '.' and expr[i] != '[') i += 1;
            if (i == start) return error.InvalidJsonPath;
            if (result.len >= 16) return error.JsonPathTooDeep;
            result.segments_buf[result.len] = .{ .key = expr[start..i] };
            result.len += 1;
        } else if (expr[i] == '[') {
            i += 1;
            const start = i;
            while (i < expr.len and expr[i] != ']') i += 1;
            if (i >= expr.len) return error.InvalidJsonPath;
            const idx = std.fmt.parseUnsigned(usize, expr[start..i], 10) catch return error.InvalidJsonPath;
            if (result.len >= 16) return error.JsonPathTooDeep;
            result.segments_buf[result.len] = .{ .index = idx };
            result.len += 1;
            i += 1; // skip ']'
        } else {
            return error.InvalidJsonPath;
        }
    }

    return result;
}

/// Extract a value from a JSON body using a path expression.
/// Returns the raw JSON value (string without quotes, or number/bool/null as-is).
pub fn extract(json: []const u8, path: JsonPath) ?[]const u8 {
    var current = json;

    for (path.segments()) |seg| {
        current = skipWhitespace(current);
        switch (seg) {
            .key => |key| {
                if (current.len == 0 or current[0] != '{') return null;
                current = findObjectValue(current, key) orelse return null;
            },
            .index => |idx| {
                if (current.len == 0 or current[0] != '[') return null;
                current = findArrayElement(current, idx) orelse return null;
            },
        }
    }

    current = skipWhitespace(current);
    // Return the raw value
    return extractValue(current);
}

/// Extract a string value (unquoted) from JSON at the given path.
pub fn extractString(json: []const u8, path: JsonPath) ?[]const u8 {
    const raw = extract(json, path) orelse return null;
    if (raw.len >= 2 and raw[0] == '"' and raw[raw.len - 1] == '"') {
        return raw[1 .. raw.len - 1];
    }
    return raw;
}

fn skipWhitespace(s: []const u8) []const u8 {
    var i: usize = 0;
    while (i < s.len and (s[i] == ' ' or s[i] == '\t' or s[i] == '\n' or s[i] == '\r')) i += 1;
    return s[i..];
}

fn findObjectValue(json: []const u8, key: []const u8) ?[]const u8 {
    if (json.len == 0 or json[0] != '{') return null;
    var i: usize = 1;

    while (i < json.len) {
        // Skip whitespace
        while (i < json.len and isWhitespace(json[i])) i += 1;
        if (i >= json.len or json[i] == '}') return null;
        if (json[i] == ',') {
            i += 1;
            continue;
        }

        // Parse key
        if (json[i] != '"') return null;
        const kstart = i + 1;
        i += 1;
        while (i < json.len and json[i] != '"') {
            if (json[i] == '\\') i += 1;
            i += 1;
        }
        if (i >= json.len) return null;
        const kend = i;
        i += 1; // skip closing quote

        // Skip whitespace and colon
        while (i < json.len and isWhitespace(json[i])) i += 1;
        if (i >= json.len or json[i] != ':') return null;
        i += 1;
        while (i < json.len and isWhitespace(json[i])) i += 1;

        // Check if this is the key we want
        if (std.mem.eql(u8, json[kstart..kend], key)) {
            return json[i..];
        }

        // Skip value
        i = skipValue(json, i) orelse return null;
    }
    return null;
}

fn findArrayElement(json: []const u8, index: usize) ?[]const u8 {
    if (json.len == 0 or json[0] != '[') return null;
    var i: usize = 1;
    var current_idx: usize = 0;

    while (i < json.len) {
        while (i < json.len and isWhitespace(json[i])) i += 1;
        if (i >= json.len or json[i] == ']') return null;
        if (json[i] == ',') {
            i += 1;
            continue;
        }

        if (current_idx == index) {
            return json[i..];
        }

        i = skipValue(json, i) orelse return null;
        current_idx += 1;
    }
    return null;
}

fn extractValue(json: []const u8) ?[]const u8 {
    if (json.len == 0) return null;

    if (json[0] == '"') {
        // String value - find closing quote
        var i: usize = 1;
        while (i < json.len and json[i] != '"') {
            if (json[i] == '\\') i += 1;
            i += 1;
        }
        if (i >= json.len) return null;
        return json[0 .. i + 1]; // include quotes
    }

    if (json[0] == '{' or json[0] == '[') {
        const end = skipValue(json, 0) orelse return null;
        return json[0..end];
    }

    // Number, bool, null
    var i: usize = 0;
    while (i < json.len and json[i] != ',' and json[i] != '}' and json[i] != ']' and !isWhitespace(json[i])) i += 1;
    if (i == 0) return null;
    return json[0..i];
}

fn skipValue(json: []const u8, start: usize) ?usize {
    if (start >= json.len) return null;
    var i = start;

    switch (json[i]) {
        '"' => {
            i += 1;
            while (i < json.len and json[i] != '"') {
                if (json[i] == '\\') i += 1;
                i += 1;
            }
            if (i >= json.len) return null;
            return i + 1;
        },
        '{' => {
            var depth: u32 = 1;
            i += 1;
            while (i < json.len and depth > 0) {
                if (json[i] == '{') depth += 1;
                if (json[i] == '}') depth -= 1;
                if (json[i] == '"') {
                    i += 1;
                    while (i < json.len and json[i] != '"') {
                        if (json[i] == '\\') i += 1;
                        i += 1;
                    }
                }
                i += 1;
            }
            return i;
        },
        '[' => {
            var depth: u32 = 1;
            i += 1;
            while (i < json.len and depth > 0) {
                if (json[i] == '[') depth += 1;
                if (json[i] == ']') depth -= 1;
                if (json[i] == '"') {
                    i += 1;
                    while (i < json.len and json[i] != '"') {
                        if (json[i] == '\\') i += 1;
                        i += 1;
                    }
                }
                i += 1;
            }
            return i;
        },
        else => {
            // Number, bool, null
            while (i < json.len and json[i] != ',' and json[i] != '}' and json[i] != ']' and !isWhitespace(json[i])) i += 1;
            return i;
        },
    }
}

fn isWhitespace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r';
}

// ---- Processor factories for JSON transformation ----

/// Create a processor that extracts a JSON path value and sets it as body.
pub fn extractToBody(allocator: std.mem.Allocator, path_expr: []const u8) !Processor {
    const path = try parsePath(path_expr);
    const Ctx = struct {
        path: JsonPath,
    };
    const ctx = try allocator.create(Ctx);
    ctx.* = .{ .path = path };
    return Processor.fromFn(struct {
        fn call(c: ?*anyopaque, ex: *Exchange) anyerror!void {
            const self: *const Ctx = @ptrCast(@alignCast(c.?));
            if (extractString(ex.body, self.path)) |val| {
                // Copy before setBody, since val may point into ex.body
                const copy = try ex.allocator.dupe(u8, val);
                defer ex.allocator.free(copy);
                try ex.setBody(copy);
            }
        }
    }.call, ctx);
}

/// Create a processor that extracts a JSON path value and sets it as a header.
pub fn extractToHeader(allocator: std.mem.Allocator, path_expr: []const u8, header_key: []const u8) !Processor {
    const path = try parsePath(path_expr);
    const Ctx = struct {
        path: JsonPath,
        header: []const u8,
    };
    const ctx = try allocator.create(Ctx);
    ctx.* = .{ .path = path, .header = header_key };
    return Processor.fromFn(struct {
        fn call(c: ?*anyopaque, ex: *Exchange) anyerror!void {
            const self: *const Ctx = @ptrCast(@alignCast(c.?));
            if (extractString(ex.body, self.path)) |val| {
                try ex.putHeader(self.header, val);
            }
        }
    }.call, ctx);
}

/// Create a processor that sets a JSON field at the given path to a value from a header.
/// Only works for top-level keys (replaces or adds).
pub fn setJsonField(allocator: std.mem.Allocator, field_key: []const u8, header_key: []const u8) !Processor {
    const Ctx = struct {
        field: []const u8,
        header: []const u8,
    };
    const ctx = try allocator.create(Ctx);
    ctx.* = .{ .field = field_key, .header = header_key };
    return Processor.fromFn(struct {
        fn call(c: ?*anyopaque, ex: *Exchange) anyerror!void {
            const self: *const Ctx = @ptrCast(@alignCast(c.?));
            const val = ex.getHeader(self.header) orelse return;

            // Simple: rebuild JSON with the field set
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(ex.allocator);

            const body = skipWhitespace(ex.body);
            if (body.len == 0 or body[0] != '{') {
                // Not an object — create new one
                try buf.appendSlice(ex.allocator, "{\"");
                try buf.appendSlice(ex.allocator, self.field);
                try buf.appendSlice(ex.allocator, "\":\"");
                try appendEscaped(&buf, ex.allocator, val);
                try buf.appendSlice(ex.allocator, "\"}");
            } else {
                // Copy existing object, replace/add field
                try buf.append(ex.allocator, '{');
                var found = false;
                var first = true;

                var i: usize = 1;
                while (i < body.len) {
                    while (i < body.len and isWhitespace(body[i])) i += 1;
                    if (i >= body.len or body[i] == '}') break;
                    if (body[i] == ',') {
                        i += 1;
                        continue;
                    }

                    // Parse key
                    if (body[i] != '"') break;
                    const kstart = i + 1;
                    i += 1;
                    while (i < body.len and body[i] != '"') {
                        if (body[i] == '\\') i += 1;
                        i += 1;
                    }
                    if (i >= body.len) break;
                    const key = body[kstart..i];
                    i += 1;

                    // Skip to value
                    while (i < body.len and isWhitespace(body[i])) i += 1;
                    if (i >= body.len or body[i] != ':') break;
                    i += 1;
                    while (i < body.len and isWhitespace(body[i])) i += 1;

                    const vstart = i;
                    i = skipValue(body, i) orelse break;

                    if (!first) try buf.append(ex.allocator, ',');
                    first = false;

                    try buf.append(ex.allocator, '"');
                    try buf.appendSlice(ex.allocator, key);
                    try buf.appendSlice(ex.allocator, "\":");

                    if (std.mem.eql(u8, key, self.field)) {
                        // Replace with new value
                        try buf.append(ex.allocator, '"');
                        try appendEscaped(&buf, ex.allocator, val);
                        try buf.append(ex.allocator, '"');
                        found = true;
                    } else {
                        try buf.appendSlice(ex.allocator, body[vstart..i]);
                    }
                }

                if (!found) {
                    if (!first) try buf.append(ex.allocator, ',');
                    try buf.append(ex.allocator, '"');
                    try buf.appendSlice(ex.allocator, self.field);
                    try buf.appendSlice(ex.allocator, "\":\"");
                    try appendEscaped(&buf, ex.allocator, val);
                    try buf.append(ex.allocator, '"');
                }
                try buf.append(ex.allocator, '}');
            }

            try ex.setBody(buf.items);
        }
    }.call, ctx);
}

fn appendEscaped(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => try buf.append(allocator, c),
        }
    }
}

// -------- tests --------

test "parsePath: simple key" {
    const path = try parsePath("$.name");
    try std.testing.expectEqual(@as(usize, 1), path.segments().len);
    try std.testing.expectEqualStrings("name", path.segments()[0].key);
}

test "parsePath: nested keys" {
    const path = try parsePath("$.data.items");
    try std.testing.expectEqual(@as(usize, 2), path.segments().len);
    try std.testing.expectEqualStrings("data", path.segments()[0].key);
    try std.testing.expectEqualStrings("items", path.segments()[1].key);
}

test "parsePath: array index" {
    const path = try parsePath("$.items[0]");
    try std.testing.expectEqual(@as(usize, 2), path.segments().len);
    try std.testing.expectEqualStrings("items", path.segments()[0].key);
    try std.testing.expectEqual(@as(usize, 0), path.segments()[1].index);
}

test "parsePath: nested array and key" {
    const path = try parsePath("$.items[2].name");
    try std.testing.expectEqual(@as(usize, 3), path.segments().len);
}

test "extract: simple string" {
    const json = "{\"name\":\"alice\",\"age\":30}";
    const path = try parsePath("$.name");
    const val = extract(json, path);
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("\"alice\"", val.?);
}

test "extractString: simple string" {
    const json = "{\"name\":\"alice\",\"age\":30}";
    const path = try parsePath("$.name");
    const val = extractString(json, path);
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("alice", val.?);
}

test "extract: number" {
    const json = "{\"name\":\"alice\",\"age\":30}";
    const path = try parsePath("$.age");
    const val = extract(json, path);
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("30", val.?);
}

test "extract: nested object" {
    const json = "{\"user\":{\"name\":\"bob\",\"id\":42}}";
    const path = try parsePath("$.user.name");
    const val = extractString(json, path);
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("bob", val.?);
}

test "extract: array element" {
    const json = "{\"items\":[\"a\",\"b\",\"c\"]}";
    const path = try parsePath("$.items[1]");
    const val = extractString(json, path);
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("b", val.?);
}

test "extract: array object element" {
    const json = "{\"users\":[{\"name\":\"x\"},{\"name\":\"y\"}]}";
    const path = try parsePath("$.users[1].name");
    const val = extractString(json, path);
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("y", val.?);
}

test "extract: missing key returns null" {
    const json = "{\"name\":\"alice\"}";
    const path = try parsePath("$.missing");
    try std.testing.expect(extract(json, path) == null);
}

test "extractToBody processor" {
    const alloc = std.testing.allocator;
    const proc = try extractToBody(alloc, "$.name");
    defer alloc.destroy(@as(*const struct { path: JsonPath }, @ptrCast(@alignCast(proc.ctx.?))));

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("{\"name\":\"alice\",\"age\":30}");

    try proc.call(&ex);
    try std.testing.expectEqualStrings("alice", ex.body);
}

test "extractToHeader processor" {
    const alloc = std.testing.allocator;
    const proc = try extractToHeader(alloc, "$.id", "userId");
    defer alloc.destroy(@as(*const struct { path: JsonPath, header: []const u8 }, @ptrCast(@alignCast(proc.ctx.?))));

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("{\"id\":\"123\",\"name\":\"test\"}");

    try proc.call(&ex);
    try std.testing.expectEqualStrings("123", ex.getHeader("userId").?);
}

test "setJsonField processor" {
    const alloc = std.testing.allocator;
    const proc = try setJsonField(alloc, "status", "myStatus");
    defer alloc.destroy(@as(*const struct { field: []const u8, header: []const u8 }, @ptrCast(@alignCast(proc.ctx.?))));

    var ex = Exchange.init(alloc);
    defer ex.deinit();
    try ex.setBody("{\"name\":\"test\"}");
    try ex.putHeader("myStatus", "done");

    try proc.call(&ex);

    // Body should now contain the status field
    try std.testing.expect(std.mem.indexOf(u8, ex.body, "\"status\":\"done\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, ex.body, "\"name\":\"test\"") != null);
}
