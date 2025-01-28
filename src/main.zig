const std = @import("std");
const parser = @import("tokenizer.zig");
const sql = @import("db.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <expr>\n", .{args[0]});
        return;
    }
    const expr = args[1];
    var tokenizer = parser.Tokenizer.init(expr, allocator);
    const tokens = try tokenizer.tokenize();

    for (tokens.items) |str| {
        std.debug.print("{s}\n", .{str.value});
    }

    var db = try sql.Db.init("db/test.db");
    defer db.deinit();
}
