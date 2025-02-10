const std = @import("std");
const Token = @import("tokenizer.zig").Token;
const TokenType = @import("tokenizer.zig").TokenType;
const sql = @import("db.zig");

const testing = std.testing;
const ArrayList = std.ArrayList;

pub const Parser = struct {
   allocator: std.mem.Allocator,
   db: sql.Db,
   tokens: ArrayList(Token),
   pos: usize,

   pub fn init(tokens: ArrayList(Token), allocator: std.mem.Allocator, db: sql.Db) Parser {
      return Parser{.tokens = tokens, .allocator = allocator, .db = db, .pos = 0};
   }

   pub fn parse(self: *Parser) !void {
      var current_token = self.tokens.items[self.pos];
      if (current_token.tokenType() == TokenType.Command) {
         try executeCommand(self, current_token);
      }
      if (current_token.tokenType() == TokenType.Keyword and std.mem.eql(u8, current_token.value, "SELECT")) {
         try executeSelect(self, 0);
      }
   }

   pub fn executeCommand(self: *Parser, command: Token) !void {
      if (std.mem.eql(u8, command.value, ".dbinfo")) {
         try self.db.printDbInfo(std.io.getStdOut().writer());
      }
      if (std.mem.eql(u8, command.value, ".schemas")) {
         try self.db.printSchemas();
      }
      if (std.mem.eql(u8, command.value, ".tables")) {
         try self.db.printTables(std.io.getStdOut().writer());
      }
   }

   // just a basic parser for the time being "SELECT * FROM {table};"
   pub fn executeSelect(self: *Parser, _: usize) !void {
      var table_name: []const u8 = "";
      for (self.tokens.items, 0..) |token, i| {
         if (std.ascii.eqlIgnoreCase(token.value, "from")) {
            table_name = self.tokens.items[i+1].value;
         }
      }
      const schemas = try self.db.read_schema();
      for (schemas) |schema| {
         if (std.mem.eql(u8, schema.tableName, table_name)) {
            const records = try self.db.scan_table(@intCast(schema.rootPage));
            for (records) |record| {
               record.print();
            }
         }
      }
   }
};