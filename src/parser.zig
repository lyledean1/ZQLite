const std = @import("std");
const Token = @import("tokenizer.zig").Token;
const TokenType = @import("tokenizer.zig").TokenType;
const sql = @import("db.zig");

const testing = std.testing;
const ArrayList = std.ArrayList;

// WHERE clause expression types
const CompareOp = enum {
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
};

const WhereExpr = struct {
    column_name: []const u8,
    op: CompareOp,
    value: []const u8,
};

// Query execution plan
const QueryPlan = union(enum) {
    FullScan,
    Seek: sql.SeekCriteria,
};

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
      if (current_token.tokenType() == TokenType.Keyword and std.ascii.eqlIgnoreCase(current_token.value, "SELECT")) {
         try executeSelect(self, 0);
      }
   }

   pub fn executeCommand(self: *Parser, command: Token) !void {
      if (std.mem.eql(u8, command.value, ".dbinfo")) {
         try self.db.printDbInfo();
      }
      if (std.mem.eql(u8, command.value, ".schema")) {
         try self.db.printSchemas();
      }
      if (std.mem.eql(u8, command.value, ".tables")) {
         try self.db.printTables();
      }
   }

   // Parse SELECT statement with optional WHERE clause
   // Supports: SELECT * FROM table [WHERE column op value]
   pub fn executeSelect(self: *Parser, _: usize) !void {
      var table_name: []const u8 = "";
      var where_expr: ?WhereExpr = null;

      // Find table name
      for (self.tokens.items, 0..) |token, i| {
         if (std.ascii.eqlIgnoreCase(token.value, "from")) {
            table_name = self.tokens.items[i+1].value;
         }
      }

      // Parse WHERE clause if present
      for (self.tokens.items, 0..) |token, i| {
         if (std.ascii.eqlIgnoreCase(token.value, "where")) {
            if (i + 3 < self.tokens.items.len) {
               const column = self.tokens.items[i + 1].value;
               const op_token = self.tokens.items[i + 2];
               const value = self.tokens.items[i + 3].value;

               const op = parseCompareOp(op_token.value) orelse CompareOp.Equal;
               where_expr = WhereExpr{
                  .column_name = column,
                  .op = op,
                  .value = value,
               };
            }
            break;
         }
      }

      // Get schema and execute query with optimization
      const schemas = try self.db.read_schema();
      for (schemas) |schema| {
         if (std.mem.eql(u8, schema.tableName, table_name)) {
            // Create query plan (decide seek vs full scan)
            const plan = if (where_expr) |expr| try self.planQuery(expr) else QueryPlan.FullScan;

            // Debug: Show query plan
            if (self.db.debug) {
               std.debug.print("\n[QUERY PLAN]\n", .{});
               if (where_expr) |expr| {
                  std.debug.print("  WHERE: {s} {s} {s}\n", .{expr.column_name, compareOpToString(expr.op), expr.value});
               }
               switch (plan) {
                  .Seek => |criteria| {
                     std.debug.print("  Strategy: B-tree SEEK (optimized)\n", .{});
                     std.debug.print("  Mode: {s}\n", .{seekModeToString(criteria.mode)});
                     std.debug.print("  Target: rowid {s} {d}\n", .{seekModeToOp(criteria.mode), criteria.key_start});
                  },
                  .FullScan => {
                     std.debug.print("  Strategy: FULL TABLE SCAN\n", .{});
                     if (where_expr) |expr| {
                        if (!isRowIdColumn(expr.column_name)) {
                           std.debug.print("  Reason: Non-indexed column '{s}'\n", .{expr.column_name});
                        }
                     }
                  },
               }
               std.debug.print("\n[EXECUTION]\n", .{});
            }

            // Execute based on plan
            const records = switch (plan) {
               .Seek => |criteria| try self.db.seek_table(@intCast(schema.rootPage), criteria),
               .FullScan => try self.db.scan_table(@intCast(schema.rootPage)),
            };

            // Filter and print results
            // (Seek already filtered by rowid, but may need additional filtering for non-rowid columns)
            for (records) |record| {
               if (where_expr) |expr| {
                  // Skip filtering if we used seek on the same column
                  const used_seek = switch (plan) {
                     .Seek => isRowIdColumn(expr.column_name),
                     .FullScan => false,
                  };

                  if (used_seek or try evaluateWhere(record, expr, schema.sql)) {
                     record.print();
                  }
               } else {
                  record.print();
               }
            }
         }
      }
   }

   // Query planner: decide whether to use B-tree seek or full scan
   fn planQuery(_: *Parser, where_expr: WhereExpr) !QueryPlan {
      // Check if WHERE clause is on PRIMARY KEY / rowid
      if (isRowIdColumn(where_expr.column_name)) {
         // We can use B-tree seeking!
         const value_int = std.fmt.parseInt(i64, where_expr.value, 10) catch {
            // If value is not an integer, fall back to full scan
            return QueryPlan.FullScan;
         };

         const criteria = sql.SeekCriteria{
            .mode = switch (where_expr.op) {
               .Equal => sql.SeekMode.Equal,
               .LessThan => sql.SeekMode.LessThan,
               .GreaterThan => sql.SeekMode.GreaterThan,
               .LessThanOrEqual => sql.SeekMode.LessEqual,
               .GreaterThanOrEqual => sql.SeekMode.GreaterEqual,
               .NotEqual => return QueryPlan.FullScan, // Can't optimize != with seek
            },
            .key_start = value_int,
         };

         return QueryPlan{ .Seek = criteria };
      }

      // Non-indexed column: use full scan
      return QueryPlan.FullScan;
   }

   fn isRowIdColumn(column_name: []const u8) bool {
      // SQLite aliases for rowid
      return std.ascii.eqlIgnoreCase(column_name, "id") or
             std.ascii.eqlIgnoreCase(column_name, "rowid") or
             std.ascii.eqlIgnoreCase(column_name, "_rowid_") or
             std.ascii.eqlIgnoreCase(column_name, "oid");
   }

   fn parseCompareOp(op: []const u8) ?CompareOp {
      if (std.mem.eql(u8, op, "=")) return CompareOp.Equal;
      if (std.mem.eql(u8, op, "!=")) return CompareOp.NotEqual;
      if (std.mem.eql(u8, op, "<>")) return CompareOp.NotEqual;
      if (std.mem.eql(u8, op, "<")) return CompareOp.LessThan;
      if (std.mem.eql(u8, op, ">")) return CompareOp.GreaterThan;
      if (std.mem.eql(u8, op, "<=")) return CompareOp.LessThanOrEqual;
      if (std.mem.eql(u8, op, ">=")) return CompareOp.GreaterThanOrEqual;
      return null;
   }

   fn evaluateWhere(record: sql.LeafTableCell, expr: WhereExpr, schema_sql: []const u8) !bool {
      // Parse schema to find column index
      const column_idx = try findColumnIndex(schema_sql, expr.column_name) orelse return false;

      if (column_idx >= record.values.len) return false;

      const column_value = record.values[column_idx];

      // Compare based on value type
      return switch (column_value) {
         .Text => |text| compareText(text, expr.value, expr.op),
         .Int8 => |val| compareInt(val, expr.value, expr.op),
         .Int16 => |val| compareInt(val, expr.value, expr.op),
         .Int24 => |val| compareInt(val, expr.value, expr.op),
         .Int32 => |val| compareInt(val, expr.value, expr.op),
         .Int48 => |val| compareInt(val, expr.value, expr.op),
         .Int64 => |val| compareInt(val, expr.value, expr.op),
         .Null => expr.op == CompareOp.Equal and std.mem.eql(u8, expr.value, "NULL"),
         else => false,
      };
   }

   fn findColumnIndex(schema_sql: []const u8, column_name: []const u8) !?usize {
      // Simple parser: find column_name in CREATE TABLE statement
      // Format: CREATE TABLE name (col1 type, col2 type, ...)

      var in_parens = false;
      var col_idx: usize = 0;
      var i: usize = 0;

      while (i < schema_sql.len) : (i += 1) {
         if (schema_sql[i] == '(') {
            in_parens = true;
            i += 1;
            break;
         }
      }

      if (!in_parens) return null;

      var start = i;
      while (i < schema_sql.len) : (i += 1) {
         if (schema_sql[i] == ' ' or schema_sql[i] == ',' or schema_sql[i] == ')') {
            const col_name = schema_sql[start..i];
            if (std.mem.eql(u8, col_name, column_name)) {
               return col_idx;
            }

            if (schema_sql[i] == ',') {
               col_idx += 1;
               // Skip whitespace after comma
               while (i + 1 < schema_sql.len and schema_sql[i + 1] == ' ') : (i += 1) {}
               start = i + 1;
            } else if (schema_sql[i] == ')') {
               break;
            } else {
               // Skip type definition until comma or )
               while (i < schema_sql.len and schema_sql[i] != ',' and schema_sql[i] != ')') : (i += 1) {}
               if (i < schema_sql.len and schema_sql[i] == ',') {
                  col_idx += 1;
                  while (i + 1 < schema_sql.len and schema_sql[i + 1] == ' ') : (i += 1) {}
                  start = i + 1;
               }
            }
         }
      }

      return null;
   }

   fn compareText(text: []const u8, value: []const u8, op: CompareOp) bool {
      // Remove quotes from value if present
      const clean_value = if (value.len >= 2 and value[0] == '\'' and value[value.len - 1] == '\'')
         value[1..value.len - 1]
      else
         value;

      return switch (op) {
         .Equal => std.mem.eql(u8, text, clean_value),
         .NotEqual => !std.mem.eql(u8, text, clean_value),
         .LessThan => std.mem.order(u8, text, clean_value) == .lt,
         .GreaterThan => std.mem.order(u8, text, clean_value) == .gt,
         .LessThanOrEqual => {
            const ord = std.mem.order(u8, text, clean_value);
            return ord == .lt or ord == .eq;
         },
         .GreaterThanOrEqual => {
            const ord = std.mem.order(u8, text, clean_value);
            return ord == .gt or ord == .eq;
         },
      };
   }

   fn compareInt(int_val: anytype, value: []const u8, op: CompareOp) bool {
      const compare_val = std.fmt.parseInt(i64, value, 10) catch return false;
      const int_val_i64: i64 = @intCast(int_val);

      return switch (op) {
         .Equal => int_val_i64 == compare_val,
         .NotEqual => int_val_i64 != compare_val,
         .LessThan => int_val_i64 < compare_val,
         .GreaterThan => int_val_i64 > compare_val,
         .LessThanOrEqual => int_val_i64 <= compare_val,
         .GreaterThanOrEqual => int_val_i64 >= compare_val,
      };
   }

   // Helper functions for debug output
   fn compareOpToString(op: CompareOp) []const u8 {
      return switch (op) {
         .Equal => "=",
         .NotEqual => "!=",
         .LessThan => "<",
         .GreaterThan => ">",
         .LessThanOrEqual => "<=",
         .GreaterThanOrEqual => ">=",
      };
   }

   fn seekModeToString(mode: sql.SeekMode) []const u8 {
      return switch (mode) {
         .Equal => "EQUAL",
         .LessThan => "LESS_THAN",
         .GreaterThan => "GREATER_THAN",
         .LessEqual => "LESS_OR_EQUAL",
         .GreaterEqual => "GREATER_OR_EQUAL",
         .Range => "RANGE",
      };
   }

   fn seekModeToOp(mode: sql.SeekMode) []const u8 {
      return switch (mode) {
         .Equal => "=",
         .LessThan => "<",
         .GreaterThan => ">",
         .LessEqual => "<=",
         .GreaterEqual => ">=",
         .Range => "IN",
      };
   }
};