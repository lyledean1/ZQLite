const std = @import("std");
const mem = std.mem;

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
            0x00, 0x2e, 0x6e, 0xba, 0x0d, 0x00, 0x00, 0x00,
            0x00, 0x0f, 0xf4, 0x00, 0x00, 0x00, 0x00, 0x00,
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
        try writer.print("text encoding:       {d}{s}\n", .{info.textEncoding, encoding_description});
        try writer.print("user version:        {d}\n", .{info.userVersion});
        try writer.print("application id:      {d}\n", .{info.applicationId});
        try writer.print("software version:    {d}\n", .{info.softwareVersion});
        // try writer.print("number of tables:    {d}\n", .{info.numberOfTables});
        // try writer.print("number of indexes:   {d}\n", .{info.numberOfIndexes});
        // try writer.print("number of triggers:  {d}\n", .{info.numberOfTriggers});
        // try writer.print("number of views:     {d}\n", .{info.numberOfViews});
        // try writer.print("schema size:         {d}\n", .{info.schemaSize});
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

pub const SchemaEntry = {};
pub const ColumnDef = {};
pub const PageHeader = {};
pub const TableRecord = {};
pub const TableRawRecord = {};
pub const InteriorTableEntry = {};
pub const InteriorIndexEntry = {};