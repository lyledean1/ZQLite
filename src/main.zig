const std = @import("std");
const Tokenizer = @import("tokenizer.zig").Tokenizer;
const sql = @import("db.zig");
const Parser = @import("parser.zig").Parser;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        std.debug.print("Usage: {s} <file> <sql> \n", .{args[0]});
        return;
    }
    const file = args[1];
    const command = args[2];

    var db = try sql.Db.open(file, allocator);
    var tokenizer = Tokenizer.init(command, allocator);
    var tokens = try tokenizer.tokenize();
    defer tokens.deinit();
    var parser = Parser.init(tokens, allocator, db);
    try parser.parse();
    defer db.deinit();
}