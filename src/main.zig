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
        defer tokens.deinit(allocator);
        var parser = Parser.init(tokens, allocator, db);
        try parser.parse();
        return;
    }
    try repl(db, allocator);
}

fn repl(db: sql.Db, allocator: std.mem.Allocator) !void {
    var stdin_buf: [4096]u8 = undefined;
    const stdin_file = std.fs.File.stdin();
    var stdin = stdin_file.reader(&stdin_buf);
    std.debug.print("Running ZQLite\n", .{});
    while (true) {
        std.debug.print("zqlite> ", .{});

        // Read line manually
        var buffer: [1024]u8 = undefined;
        var idx: usize = 0;
        while (idx < buffer.len) : (idx += 1) {
            var byte: [1]u8 = undefined;
            const n = stdin.read(&byte) catch |err| {
                if (err == error.EndOfStream) return;
                return err;
            };
            if (n == 0) return;
            if (byte[0] == '\n') break;
            buffer[idx] = byte[0];
        }
        const input = buffer[0..idx];

        if (std.mem.eql(u8, input, "exit")) {
            break;
        }
        var tokenizer = Tokenizer.init(input, allocator);
        var tokens = try tokenizer.tokenize();
        defer tokens.deinit(allocator);
        var parser = Parser.init(tokens, allocator, db);
        try parser.parse();
    }
}