const std = @import("std");
const parser = @import("tokenizer.zig");
const sql = @import("db.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <file> \n", .{args[0]});
        return;
    }
    const file = args[1];
    // var tokenizer = parser.Tokenizer.init(expr, allocator);
    // const tokens = try tokenizer.tokenize();
    //
    // for (tokens.items) |str| {
    //     std.debug.print("{s}\n", .{str.value});
    // }

    var db = try sql.Db.open(file);
    try db.readInfo();
    try db.printDbInfo(std.io.getStdOut().writer());
    const records = try db.scan_table(allocator, 2);
    for (records) |record| {
        record.print();
        // for (record.values) |value| {
        //     sql.printColumnValue(value);
        // }
    }
    // const records_two = try db.scan_table(allocator, 2);
    // for (records_two) |record| {
    //     record.print();
    //     // for (record.values) |value| {
    //     //     sql.printColumnValue(value);
    //     // }
    // }
    defer db.deinit();
}