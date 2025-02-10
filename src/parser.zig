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
};