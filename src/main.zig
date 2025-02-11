const std = @import("std");
const Tokenizer = @import("tokenizer.zig").Tokenizer;
const sql = @import("db.zig");
const Parser = @import("parser.zig").Parser;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <file> <sql> \n", .{args[0]});
        return;
    }
    const file = args[1];
    var db = try sql.Db.open(file, allocator);
    defer db.deinit();

    if (args.len > 2) {
        const command = args[2];
        var tokenizer = Tokenizer.init(command, allocator);
        var tokens = try tokenizer.tokenize();
        defer tokens.deinit();
        var parser = Parser.init(tokens, allocator, db);
        try parser.parse();
        return;
    }
    try repl(db, allocator);
}

fn repl(db: sql.Db, allocator: std.mem.Allocator) !void {
    const stdin = std.io.getStdIn().reader();
    const stdout = std.io.getStdOut().writer();
    var buffer: [1024]u8 = undefined;
    try stdout.print("Running ZQLite\n", .{});
    while (true) {
        try stdout.print("zqlite> ", .{});
        const input = (try stdin.readUntilDelimiterOrEof(&buffer, '\n')) orelse break;
        if (std.mem.eql(u8, input, "exit")) {
            break;
        }
        var tokenizer = Tokenizer.init(input, allocator);
        var tokens = try tokenizer.tokenize();
        defer tokens.deinit();
        var parser = Parser.init(tokens, allocator, db);
        try parser.parse();
    }
}