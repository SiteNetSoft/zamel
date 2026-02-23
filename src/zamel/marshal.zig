const std = @import("std");
const Exchange = @import("exchange.zig").Exchange;

/// Marshal: collect all headers into a JSON object string and set as body.
pub fn marshalJson(ex: *Exchange) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(ex.allocator);

    try buf.append(ex.allocator, '{');
    var first = true;
    var it = ex.headers.iterator();
    while (it.next()) |entry| {
        if (!first) try buf.append(ex.allocator, ',');
        first = false;
        try buf.append(ex.allocator, '"');
        try appendEscaped(&buf, ex.allocator, entry.key_ptr.*);
        try buf.appendSlice(ex.allocator, "\":\"");
        try appendEscaped(&buf, ex.allocator, entry.value_ptr.*);
        try buf.append(ex.allocator, '"');
    }
    try buf.append(ex.allocator, '}');

    const json = try ex.allocator.dupe(u8, buf.items);
    if (ex.body.len != 0) ex.allocator.free(ex.body);
    ex.body = json;
}

/// Unmarshal: parse body as JSON object, extract top-level string fields into headers.
pub fn unmarshalJson(ex: *Exchange) !void {
    const body = ex.body;
    if (body.len < 2) return;

    var i: usize = 0;

    // Skip whitespace
    while (i < body.len and (body[i] == ' ' or body[i] == '\t' or body[i] == '\n' or body[i] == '\r')) i += 1;
    if (i >= body.len or body[i] != '{') return;
    i += 1;

    while (i < body.len) {
        // Skip whitespace
        while (i < body.len and (body[i] == ' ' or body[i] == '\t' or body[i] == '\n' or body[i] == '\r')) i += 1;
        if (i >= body.len or body[i] == '}') break;

        // Skip comma
        if (body[i] == ',') {
            i += 1;
            continue;
        }

        // Parse key
        if (body[i] != '"') break;
        const key = parseJsonString(body, &i) orelse break;

        // Skip colon
        while (i < body.len and (body[i] == ' ' or body[i] == '\t' or body[i] == '\n' or body[i] == '\r')) i += 1;
        if (i >= body.len or body[i] != ':') break;
        i += 1;

        // Skip whitespace
        while (i < body.len and (body[i] == ' ' or body[i] == '\t' or body[i] == '\n' or body[i] == '\r')) i += 1;

        // Parse value (only handle strings)
        if (i >= body.len or body[i] != '"') break;
        const value = parseJsonString(body, &i) orelse break;

        try ex.putHeader(key, value);
    }
}

fn parseJsonString(body: []const u8, pos: *usize) ?[]const u8 {
    var i = pos.*;
    if (i >= body.len or body[i] != '"') return null;
    i += 1;
    const start = i;
    while (i < body.len and body[i] != '"') {
        if (body[i] == '\\') {
            i += 1;
        }
        i += 1;
    }
    if (i >= body.len) return null;
    const result = body[start..i];
    i += 1;
    pos.* = i;
    return result;
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

test "marshalJson creates JSON from headers" {
    const alloc = std.testing.allocator;

    var ex = Exchange.init(alloc);
    defer ex.deinit();

    try ex.putHeader("name", "alice");

    try marshalJson(&ex);

    try std.testing.expect(std.mem.indexOf(u8, ex.body, "\"name\":\"alice\"") != null);
}

test "unmarshalJson parses JSON into headers" {
    const alloc = std.testing.allocator;

    var ex = Exchange.init(alloc);
    defer ex.deinit();

    try ex.setBody("{\"color\":\"blue\",\"size\":\"large\"}");

    try unmarshalJson(&ex);

    try std.testing.expectEqualStrings("blue", ex.getHeader("color").?);
    try std.testing.expectEqualStrings("large", ex.getHeader("size").?);
}

test "marshal then unmarshal roundtrip" {
    const alloc = std.testing.allocator;

    var ex = Exchange.init(alloc);
    defer ex.deinit();

    try ex.putHeader("key1", "val1");

    try marshalJson(&ex);
    const json_body = try alloc.dupe(u8, ex.body);
    defer alloc.free(json_body);

    var ex2 = Exchange.init(alloc);
    defer ex2.deinit();
    try ex2.setBody(json_body);

    try unmarshalJson(&ex2);
    try std.testing.expectEqualStrings("val1", ex2.getHeader("key1").?);
}

test "unmarshalJson handles empty body" {
    const alloc = std.testing.allocator;

    var ex = Exchange.init(alloc);
    defer ex.deinit();

    try unmarshalJson(&ex);
}
