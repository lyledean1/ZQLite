const std = @import("std");
const mem = std.mem;

const PageType = enum(u8) {
    interior_table = 0x05,
    interior_index = 0x02,
    leaf_table = 0x0d,
    leaf_index = 0x0a,
    free_list = 0x00,
    free_list_trunk = 0x01,

    // If you need to handle unknown types
    pub fn fromByte(byte: u8) ?PageType {
        return switch (byte) {
            0x05 => .interior_table,
            0x02 => .interior_index,
            0x0d => .leaf_table,
            0x0a => .leaf_index,
            0x00 => .free_list,
            0x01 => .free_list_trunk,
            else => null,
        };
    }
};

const PageHeader = struct {
    page: []const u8,
    page_offset: u8,
    page_type: u8,
    first_free_block: u16,
    cell_count: u16,
    start_of_cell_content_area: u32,
    fragmented_free_bytes: u8,
    cell_pointer_array_offset: u32,
    right_most_pointer: u32,
    unallocated_region_size: u32,
    min_overflow_payload_size: u32,
    max_overflow_payload_size: u32,

    pub fn print(self: *const PageHeader) void {
        std.debug.print("Page offset: {}\n", .{self.page_offset});
        std.debug.print("Page type: 0x{x}\n", .{self.page_type});
        std.debug.print("Min Overflow Page Size: {}\n", .{self.min_overflow_payload_size});
        std.debug.print("Max Overflow Page Size: {}\n", .{self.max_overflow_payload_size});
        std.debug.print("First Free Block: 0x{x}\n", .{self.first_free_block});
        std.debug.print("Cell Count: {}\n", .{self.cell_count});
        std.debug.print("Start Of Cell Content Area: {}\n", .{self.start_of_cell_content_area});
        std.debug.print("Fragmented Free Bytes: {}\n", .{self.start_of_cell_content_area});
    }
};

pub const Db = struct {
    file: std.fs.File,
    info: ?DbInfo = null,

    pub fn new(filename: []const u8) !Db {
        const file = try std.fs.cwd().createFile(
            filename,
            .{ .read = true, .truncate = true },
        );
        errdefer file.close();

        const header = [_]u8{
            // 0x00-0x0F: Magic header
            0x53, 0x51, 0x4C, 0x69, 0x74, 0x65, 0x20, 0x66,
            0x6F, 0x72, 0x6D, 0x61, 0x74, 0x20, 0x33, 0x00,
            // 0x10-0x17: Page size and settings
            0x10, 0x00, 0x01, 0x01, 0x0c, 0x40, 0x20, 0x20,
            // 0x18-0x5F: Database settings
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        };

        // Start of page 1 after header (0x60 onwards)
        const page1 = [_]u8{
            0x00, 0x2e, 0x6e, 0xba, @intFromEnum(PageType.leaf_table), 0x00, 0x00, 0x00,
            0x00, 0x0f, 0xf4, 0x00, 0x00,                              0x00, 0x00, 0x00,
        };

        try file.writeAll(&header);
        try file.writeAll(&page1);

        // Fill rest with zeros
        var remaining: usize = 4096 - (header.len + page1.len);
        const zeros = [_]u8{0} ** 100;
        while (remaining > 0) {
            const to_write = @min(remaining, zeros.len);
            try file.writeAll(zeros[0..to_write]);
            remaining -= to_write;
        }

        return Db{
            .file = file,
        };
    }

    pub fn open(filename: []const u8) !Db {
        const file = try std.fs.cwd().openFile(filename, .{ .mode = .read_only });
        errdefer file.close();
        return Db{ .file = file };
    }

    pub fn readInfo(self: *Db) !void {
        self.info = try readDbInfo(self);
    }

    pub fn get_page(self: *Db, allocator: std.mem.Allocator, page_number: u32) !PageHeader {
        const info = self.info orelse return error.NoDbInfo;
        var page_offset: u8 = 0;
        if (page_number < 1) {
            return error.InvalidPageNumber;
        }
        if (page_number == 1) {
            page_offset += 100;
        }
        // const seek_to = (page_number) * 4096;
        const page_size: u32 = @intCast(info.databasePageSize);
        const page = try self.read_range(allocator, (page_size*page_number) - page_size + page_offset, page_size*page_number);

        // // defer allocator.free(page); todo: handle this memory deinit somewhere?
        // _ = try self.file.readAll(page);

        // see https://www.sqlite.org/fileformat2.html#b_tree_pages for more details
        var page_header: PageHeader = undefined;
        const page_type = page[0];
        page_header.page = page;
        page_header.page_offset = page_offset;
        page_header.page_type = page_type;
        page_header.min_overflow_payload_size = ((info.usablePageSize - 12) * 32 / 255) - 23;
        page_header.max_overflow_payload_size = try get_max_overload_payload_size(page_type, info.usablePageSize);
        page_header.first_free_block = std.mem.readInt(u16, page[1..3][0..2], .big);
        page_header.cell_count = std.mem.readInt(u16, page[3..5][0..2], .big);
        page_header.start_of_cell_content_area = std.mem.readInt(u16, page[5..7][0..2], .big);
        if (page_header.start_of_cell_content_area == 0) {
            page_header.start_of_cell_content_area = 65536;
        }
        page_header.fragmented_free_bytes = page[7];
        page_header.cell_pointer_array_offset = 8;
        if (page_type == 0x02 or page_type == 0x05) {
                page_header.right_most_pointer = std.mem.readInt(u16, page[8..12][0..2], .big);
                page_header.cell_pointer_array_offset += 4;
        }
        page_header.unallocated_region_size = page_header.start_of_cell_content_area - (page_header.cell_pointer_array_offset + (page_header.cell_count * 2));
        page_header.cell_pointer_array_offset += page_offset;
        return page_header;
    }

    pub fn read_range(self: *Db, allocator:  std.mem.Allocator, from: usize, to: usize) ![]const u8 {
        const buffer = try allocator.alloc(u8, to - from);
        try self.file.seekTo(from);
        const bytes_read = try self.file.readAll(buffer);
        if (bytes_read != buffer.len) {
            return error.UnexpectedEOF;
        }
        return buffer;
    }

    pub fn printDbInfo(self: *Db, writer: anytype) !void {
        const info = self.info orelse return error.NoDbInfo;

        // Get encoding description
        const encoding_description = switch (info.textEncoding) {
            1 => " (utf8)",
            2 => " (utf16le)",
            3 => " (utf16be)",
            else => "",
        };

        // Print all fields
        try writer.print("database page size:  {d}\n", .{info.databasePageSize});
        try writer.print("write format:        {d}\n", .{info.writeFormat});
        try writer.print("read format:         {d}\n", .{info.readFormat});
        try writer.print("reserved bytes:      {d}\n", .{info.reservedBytes});
        try writer.print("file change counter: {d}\n", .{info.fileChangeCounter});
        try writer.print("database page count: {d}\n", .{info.databasePageCount});
        try writer.print("freelist page count: {d}\n", .{info.freelistPageCount});
        try writer.print("schema cookie:       {d}\n", .{info.schemaCookie});
        try writer.print("schema format:       {d}\n", .{info.schemaFormat});
        try writer.print("default cache size:  {d}\n", .{info.defaultCacheSize});
        try writer.print("autovacuum top root: {d}\n", .{info.autovacuumTopRoot});
        try writer.print("incremental vacuum:  {d}\n", .{info.incrementalVacuum});
        try writer.print("text encoding:       {d}{s}\n", .{ info.textEncoding, encoding_description });
        try writer.print("user version:        {d}\n", .{info.userVersion});
        try writer.print("application id:      {d}\n", .{info.applicationId});
        try writer.print("software version:    {d}\n", .{info.softwareVersion});
        // try writer.print("number of tables:    {d}\n", .{info.numberOfTables});
        // try writer.print("number of indexes:   {d}\n", .{info.numberOfIndexes});
        // try writer.print("number of triggers:  {d}\n", .{info.numberOfTriggers});
        // try writer.print("number of views:     {d}\n", .{info.numberOfViews});
        // try writer.print("schema size:         {d}\n", .{info.schemaSize});
    }

    pub fn scan_table(self: *Db, allocator: std.mem.Allocator, root_page: u32) ![]LeafTableCell {
        var table_data = std.ArrayList(LeafTableCell).init(allocator);
        errdefer table_data.deinit();
        try self.walk_btree_table_pages(allocator, root_page, &table_data);
        return table_data.toOwnedSlice();
    }

    fn walk_btree_table_pages(self: *Db, allocator: std.mem.Allocator, page: u32, table_data: *std.ArrayList(LeafTableCell)) !void {
        const page_header = try self.get_page(allocator, page);
        switch (page_header.page_type) {
            0x05 => {
                return error.NotImplementedForPageType0x05;
            },
            0x0d => {
                const records = try self.walk_leaf_page(allocator, page_header);
                for (records) |record| {
                    try table_data.append(record);
                }
            },
            0x02 => {
                return error.NotImplementedForPageType0x02;
            },
            0x0a => {
                return error.NotImplementedForPageType0x0a;
            },
            else => {
                return error.UnknownPageType;
            }
        }
    }

    fn walk_leaf_page(_: *Db, allocator: std.mem.Allocator, page_header: PageHeader) ![]LeafTableCell {
        var list = std.ArrayList(LeafTableCell).init(allocator);
        std.debug.print("Cell here: {}\n", .{1});
        const page = page_header.page;
        if (page[0] != @intFromEnum(PageType.leaf_table)) {
            return error.NotALeafPage;
        }

        const cell_count = page_header.cell_count;
        // std.debug.print("Cell count: {}\n", .{cell_count});

        var cell_index: usize = 0;
        while (cell_index < cell_count) : (cell_index += 1) {
            const pointer_offset = cell_count + (cell_index * 2);
            const cell_offset = std.mem.readInt(u16, page[pointer_offset..][0..2], .big);

            // std.debug.print("\nCell {}: offset=0x{x:0>4}\n", .{cell_index, cell_offset});

            var stream = std.io.fixedBufferStream(page[cell_offset..]);
            const reader = stream.reader();

            const payload_size = try readVarint(reader);
            // pass through row id
        _ = try readVarint(reader);

            // std.debug.print("  Payload size: {}, Row ID: {}\n", .{payload_size, row_id});

            // Dump the raw payload for debugging
        const payload_data = page[cell_offset + stream.pos .. cell_offset + stream.pos + @as(usize, @intCast(payload_size))];
            // std.debug.print("Raw payload: ", .{});
            // for (payload_data[0..@min(payload_data.len, 32)]) |byte| {
            //     std.debug.print("{x:0>2} ", .{byte});
            // }
            // std.debug.print("\n", .{});

            const record = try parseRecord(allocator, payload_data);
            try list.append(record);
        }
        return list.toOwnedSlice();
    }

    pub fn deinit(self: *Db) void {
        self.file.close();
    }
};

fn readDbInfo(db: *Db) !DbInfo {
    var header: [100]u8 = undefined;
    const bytes_read = try db.file.read(&header);
    if (bytes_read != header.len) {
        return error.InvalidRead;
    }

    // Check SQLite header magic string
    const magic = "SQLite format 3\x00";
    if (!mem.eql(u8, header[0..16], magic)) {
        return error.InvalidSqliteFile;
    }

    // Read page size (big endian u16)
    var info: DbInfo = undefined;
    const page_size = mem.readInt(u16, header[16..18], .big);
    info.databasePageSize = if (page_size == 1) 65536 else page_size;

    // Read individual bytes
    info.writeFormat = header[18];
    info.readFormat = header[19];
    info.reservedBytes = header[20];
    info.maxEmbeddedPayloadFraction = header[21];
    info.minEmbeddedPayloadFraction = header[22];
    info.leafPayloadFraction = header[23];

    // Read big endian u32 values
    info.fileChangeCounter = mem.readInt(u32, header[24..28], .big);
    info.databasePageCount = mem.readInt(u32, header[28..32], .big);
    info.firstFreeListPage = mem.readInt(u32, header[32..36], .big);
    info.freelistPageCount = mem.readInt(u32, header[36..40], .big);
    info.schemaCookie = mem.readInt(u32, header[40..44], .big);
    info.schemaFormat = mem.readInt(u32, header[44..48], .big);
    info.defaultCacheSize = mem.readInt(u32, header[48..52], .big);
    info.autovacuumTopRoot = mem.readInt(u32, header[52..56], .big);
    info.textEncoding = mem.readInt(u32, header[56..60], .big);
    info.userVersion = mem.readInt(u32, header[60..64], .big);
    info.incrementalVacuum = mem.readInt(u32, header[64..68], .big);
    info.applicationId = mem.readInt(u32, header[68..72], .big);
    info.versionValidForNumber = mem.readInt(u32, header[92..96], .big);
    info.softwareVersion = mem.readInt(u32, header[96..100], .big);

    // Calculate usable page size
    info.usablePageSize = @intCast(info.databasePageSize - info.reservedBytes);

    return info;
}

const DbInfo = struct {
    databasePageSize: i32,
    writeFormat: u8,
    readFormat: u8,
    reservedBytes: u8,
    maxEmbeddedPayloadFraction: u8,
    minEmbeddedPayloadFraction: u8,
    leafPayloadFraction: u8,
    fileChangeCounter: u32,
    databasePageCount: u32,
    firstFreeListPage: u32,
    freelistPageCount: u32,
    schemaCookie: u32,
    schemaFormat: u32,
    defaultCacheSize: u32,
    autovacuumTopRoot: u32,
    incrementalVacuum: u32,
    textEncoding: u32,
    userVersion: u32,
    applicationId: u32,
    softwareVersion: u32,
    numberOfTables: u32,
    numberOfIndexes: u32,
    numberOfTriggers: u32,
    numberOfViews: u32,
    schemaSize: u32,
    versionValidForNumber: u32,
    usablePageSize: u32,
};

const SchemaEntry = struct { schemaType: []const u8, name: []const u8, tableName: []const u8, rootPage: u8, columns: []ColumnDef, constraints: [][]const u8 };
const ColumnDef = struct { name: []const u8, columnType: []const u8, constraints: [][]const u8 };
const TableRecord = struct {

};
pub const TableRawRecord = {};
pub const InteriorTableEntry = {};
pub const InteriorIndexEntry = {};

const SqliteType = enum(u8) {
    Null = 0,
    Int8 = 1,
    Int16 = 2,
    Int24 = 3,
    Int32 = 4,
    Int48 = 5,
    Int64 = 6,
    Float64 = 7,
    Zero = 8,
    One = 9,
    Blob = 12,
    Text = 13,
};

const ColumnValue = union(SqliteType) {
    Null: void,
    Int8: i8,
    Int16: i16,
    Int24: i24,
    Int32: i32,
    Int48: i48,
    Int64: i64,
    Float64: f64,
    Zero: void,
    One: void,
    Blob: []const u8,
    Text: []const u8,
};

const LeafTableCell = struct {
    payload_size: u64,
    row_id: u64,
    column_count: u64,
    values: []ColumnValue,

    pub fn deinit(self: *LeafTableCell, allocator: std.mem.Allocator) void {
        allocator.free(self.values);
    }

    pub fn print(self: LeafTableCell) void {
        // std.debug.print("Record ID: {}\n", .{self.row_id});
        // std.debug.print("Column count: {}\n", .{self.column_count});

        for (self.values, 0..) |value, i| {
            std.debug.print("Column {}: ", .{i});
            switch (value) {
                .Null => std.debug.print("NULL", .{}),
                .Int8 => |v| std.debug.print("{}", .{v}),
                .Int16 => |v| std.debug.print("{}", .{v}),
                .Int24 => |v| std.debug.print("{}", .{v}),
                .Int32 => |v| std.debug.print("{}", .{v}),
                .Int48 => |v| std.debug.print("{}", .{v}),
                .Int64 => |v| std.debug.print("{}", .{v}),
                .Float64 => |v| std.debug.print("{d}", .{v}),
                .Zero => std.debug.print("0", .{}),
                .One => std.debug.print("1", .{}),
                .Blob => |v| std.debug.print("BLOB({})", .{v.len}),
                .Text => |v| std.debug.print("'{s}'", .{v}),
            }
            std.debug.print("\n", .{});
        }
    }
};

fn getTypeFromSerial(serial_type: u64) !SqliteType {
    return switch (serial_type) {
        0 => .Null,
        1 => .Int8,
        2 => .Int16,
        3 => .Int24,
        4 => .Int32,
        5 => .Int48,
        6 => .Int64,
        7 => .Float64,
        8 => .Zero,
        9 => .One,
        10, 11 => return error.Reserved,
        12 => .Blob,
        13...219 => .Text, // String lengths are encoded in the type
        else => {
            if (serial_type % 2 == 0) {
                return .Blob;
            } else {
                return .Text;
            }
        },
    };
}

fn readValue(allocator: std.mem.Allocator, reader: anytype, typ: SqliteType, serial_type: u64) !ColumnValue {
    return switch (typ) {
        .Null => .{ .Null = {} },
        .Int8 => .{ .Int8 = @as(i8, @bitCast(try reader.readByte())) },
        .Int16 => .{ .Int16 = try reader.readInt(i16, .big) },
        .Int24 => {
            var bytes: [3]u8 = undefined;
            _ = try reader.readAll(&bytes);
            const val = std.mem.readInt(i24, &bytes, .big);
            return .{ .Int24 = val };
        },
        .Int32 => .{ .Int32 = try reader.readInt(i32, .big) },
        .Int48 => {
            var bytes: [6]u8 = undefined;
            _ = try reader.readAll(&bytes);
            const val = std.mem.readInt(i48, &bytes, .big);
            return .{ .Int48 = val };
        },
        .Int64 => .{ .Int64 = try reader.readInt(i64, .big) },
        .Float64 => {
            var bytes: [8]u8 = undefined;
            _ = try reader.readAll(&bytes);
            const val = std.mem.readInt(u64, &bytes, .big);
            return .{ .Float64 = @bitCast(val) };
        },
        .Zero => .{ .Zero = {} },
        .One => .{ .One = {} },
        .Text => {
            const len = (serial_type - 13) / 2;
            const buf = try allocator.alloc(u8, @intCast(len));
            _ = try reader.readAll(buf);
            return .{ .Text = buf };
        },
        .Blob => {
            @panic("Blob not implemented yet");
        },
    };
}

fn parseRecord(allocator: std.mem.Allocator, data: []const u8) !LeafTableCell {
    var stream = std.io.fixedBufferStream(data);
    const reader = stream.reader();

    const header_size = try readVarint(reader);
    _ = try readVarint(reader); // Skip the 0x00 byte
    const first_type = try readVarint(reader); // This is our actual type (0x13)
    const column_count = 1;

    // std.debug.print("Header size: {}, First type: {x}\n", .{header_size, first_type});

    var types = try std.ArrayList(SqliteType).initCapacity(allocator, column_count);
    defer types.deinit();

    const sqlite_type = try getTypeFromSerial(first_type);
    try types.append(sqlite_type);

    var values = try allocator.alloc(ColumnValue, types.items.len);
    errdefer allocator.free(values);

    for (types.items, 0..) |typ, i| {
        values[i] = try readValue(allocator, reader, typ, first_type);
    }

    return LeafTableCell{
        .payload_size = header_size,
        .row_id = 1,
        .column_count = column_count,
        .values = values,
    };
}

fn readVarint(reader: anytype) !u64 {
    var result: u64 = 0;
    var shift: u6 = 0;

    while (true) {
        const byte = try reader.readByte();
        result |= @as(u64, byte & 0x7F) << shift;

        if (byte & 0x80 == 0) break;
        shift += 7;
        if (shift >= 64) return error.VarintTooBig;
    }

    return result;
}

pub fn printColumnValue(value: ColumnValue) void {
    switch (value) {
        .Null => std.debug.print("NULL\n", .{}),
        .Int8 => |v| std.debug.print("Int8: {}\n", .{v}),
        .Int16 => |v| std.debug.print("Int16: {}\n", .{v}),
        .Int24 => |v| std.debug.print("Int24: {}\n", .{v}),
        .Int32 => |v| std.debug.print("Int32: {}\n", .{v}),
        .Int48 => |v| std.debug.print("Int48: {}\n", .{v}),
        .Int64 => |v| std.debug.print("Int64: {}\n", .{v}),
        .Float64 => |v| std.debug.print("Float64: {d}\n", .{v}),
        .Zero => std.debug.print("Zero\n", .{}),
        .One => std.debug.print("One\n", .{}),
        .Blob => |v| std.debug.print("Blob: {any}\n", .{v}),
        .Text => |v| std.debug.print("Text: {s}\n", .{v}),
    }
}

fn get_max_overload_payload_size(page_type: u32, usable_page_size: u32) !u32 {
    switch (page_type) {
        0x02, 0x0a => {
            return ((usable_page_size - 12) * 64 / 255) - 23;
        },
        0x0d, 0x05 => {
            return (usable_page_size - 35);
        },
        else => {
            return error.InvalidPageSize;
        }
    }
}