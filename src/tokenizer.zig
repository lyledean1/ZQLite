const std = @import("std");
const StaticStringMap = @import("std").StaticStringMap;
const testing = std.testing;
const ArrayList = std.ArrayList;

pub const TokenType = enum {
    Command,
    Keyword,
    Identifier,
    String,
    Number,
    Operator,
    Delimiter,
    Whitespace,
    EOF,
};

pub const Token = struct {
    type: TokenType,
    value: []const u8,
    line: usize,
    column: usize,

    pub fn init(token_type: TokenType, value: []const u8, line: usize, column: usize) Token {
        return Token{
            .type = token_type,
            .value = value,
            .line = line,
            .column = column,
        };
    }

    pub fn tokenType(self: *Token) TokenType {
        return self.type;
    }
};

pub const Tokenizer = struct {
    input: []const u8,
    pos: usize,
    line: usize,
    column: usize,
    allocator: std.mem.Allocator,

    const keywords = StaticStringMap(void).initComptime(.{
        .{ "SELECT", {} },
        .{ "FROM", {} },
        .{ "WHERE", {} },
        .{ "INSERT", {} },
        .{ "INTO", {} },
        .{ "VALUES", {} },
        .{ "UPDATE", {} },
        .{ "DELETE", {} },
        .{ "AND", {} },
        .{ "OR", {} },
    });

    const commands = StaticStringMap(void).initComptime(.{
        .{".dbinfo", {}},
        .{".tables", {}},
        .{".schema", {}}
    });

    pub fn init(input: []const u8, allocator: std.mem.Allocator) Tokenizer {
        return Tokenizer{
            .input = input,
            .pos = 0,
            .line = 1,
            .column = 1,
            .allocator = allocator,
        };
    }

    fn peek(self: *Tokenizer) ?u8 {
        if (self.pos >= self.input.len) return null;
        return self.input[self.pos];
    }

    fn advance(self: *Tokenizer) void {
        if (self.pos >= self.input.len) return;
        if (self.input[self.pos] == '\n') {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
        self.pos += 1;
    }

    fn isPeriod(c: u8) bool {
        return c == '.';
    }

    fn isWhitespace(c: u8) bool {
        return c == ' ' or c == '\t' or c == '\n' or c == '\r';
    }

    fn isAlpha(c: u8) bool {
        return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_';
    }

    fn isDigit(c: u8) bool {
        return c >= '0' and c <= '9';
    }

    fn isAlphanumeric(c: u8) bool {
        return isAlpha(c) or isDigit(c);
    }

    pub fn nextToken(self: *Tokenizer) !Token {
        while (self.peek()) |c| {
            if (!isWhitespace(c)) break;
            self.advance();
        }

        const start_pos = self.pos;
        const start_line = self.line;
        const start_column = self.column;

        const current = self.peek() orelse {
            return Token.init(TokenType.EOF, "", start_line, start_column);
        };

        if (isPeriod(current) and start_pos == 0) {
            while (self.peek()) |c| {
                if (!isAlpha(c) and c != '.') break;
                self.advance();
            }
            return Token.init(TokenType.Command, self.input[start_pos..self.pos], start_line, start_column);
        }

        if (isAlpha(current)) {
            while (self.peek()) |c| {
                if (!isAlphanumeric(c)) break;
                self.advance();
            }

            const value = self.input[start_pos..self.pos];
            const uppercased = try std.ascii.allocUpperString(self.allocator, value);
            defer self.allocator.free(uppercased);

            if (keywords.has(uppercased)) {
                return Token.init(TokenType.Keyword, value, start_line, start_column);
            }
            return Token.init(TokenType.Identifier, value, start_line, start_column);
        }

        if (isDigit(current)) {
            while (self.peek()) |c| {
                if (!isDigit(c) and c != '.') break;
                self.advance();
            }
            return Token.init(TokenType.Number, self.input[start_pos..self.pos], start_line, start_column);
        }

        if (current == '\'') {
            self.advance();
            while (self.peek()) |c| {
                if (c == '\'') break;
                self.advance();
            }
            if (self.peek()) |_| self.advance(); // Skip closing quote
            return Token.init(TokenType.String, self.input[start_pos..self.pos], start_line, start_column);
        }

        const operators = "+-*/<>=!";
        const delimiters = "(),;";

        if (std.mem.indexOfScalar(u8, operators, current) != null) {
            self.advance();
            if (self.peek()) |next| {
                if (next == '=' or (current == '<' and next == '>')) {
                    self.advance();
                }
            }
            return Token.init(TokenType.Operator, self.input[start_pos..self.pos], start_line, start_column);
        }

        if (std.mem.indexOfScalar(u8, delimiters, current) != null) {
            self.advance();
            return Token.init(TokenType.Delimiter, self.input[start_pos..self.pos], start_line, start_column);
        }

        self.advance();
        return Token.init(TokenType.Delimiter, self.input[start_pos..self.pos], start_line, start_column);
    }

    pub fn tokenize(self: *Tokenizer) !ArrayList(Token) {
        var tokens = try ArrayList(Token).initCapacity(self.allocator, 0);
        errdefer tokens.deinit(self.allocator);

        while (true) {
            const token = try self.nextToken();
            try tokens.append(self.allocator, token);
            if (token.type == TokenType.EOF) break;
        }

        return tokens;
    }
};

test "basic tokenization" {
    const allocator = testing.allocator;
    const input = "SELECT id, name FROM users WHERE age >= 18;";
    var tokenizer = Tokenizer.init(input, allocator);
    var tokens = try tokenizer.tokenize();
    defer tokens.deinit();

    try testing.expect(tokens.items.len > 0);
    try testing.expectEqual(TokenType.Keyword, tokens.items[0].type);
    try testing.expectEqualStrings("SELECT", tokens.items[0].value);
}

test "string literal tokenization" {
    const allocator = testing.allocator;
    const input = "SELECT * FROM users WHERE name = 'John Doe';";
    var tokenizer = Tokenizer.init(input, allocator);
    var tokens = try tokenizer.tokenize();
    defer tokens.deinit();

    var found_string = false;
    for (tokens.items) |token| {
        if (token.type == TokenType.String and std.mem.eql(u8, token.value, "'John Doe'")) {
            found_string = true;
            break;
        }
    }
    try testing.expect(found_string);
}

test "command tokenization" {
    const allocator = testing.allocator;
    const input = ".dbinfo;";
    var tokenizer = Tokenizer.init(input, allocator);
    var tokens = try tokenizer.tokenize();
    defer tokens.deinit();

    var found_string = false;
    for (tokens.items) |token| {
        if (token.type == TokenType.Command and std.mem.eql(u8, token.value, ".dbinfo")) {
            found_string = true;
            break;
        }
    }
    try testing.expectEqual(tokens.items.len, 3);
    try testing.expect(found_string);
}