const std = @import("std");

pub const Db = struct {
    file: []const u8,

    pub fn init(file: []const u8) Db {
        return Db{
            .file = file
        };
    }
};


pub const DbInfo = {};
pub const SchemaEntry = {};
pub const ColumnDef = {};
pub const PageHeader = {};
pub const TableRecord = {};
pub const TableRawRecord = {};
pub const InteriorTableEntry = {};
pub const InteriorIndexEntry = {};