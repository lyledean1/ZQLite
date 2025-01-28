const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn BTreeNode(comptime T: type, comptime order: u32) type {
    return struct {
        const Self = @This();
        const MinKeys = order - 1;
        const MaxKeys = 2 * order - 1;

        keys: [MaxKeys]T,
        children: [MaxKeys + 1]?*Self,
        num_keys: u32,
        is_leaf: bool,
        allocator: Allocator,

        pub fn init(allocator: Allocator) !*Self {
            const node = try allocator.create(Self);
            node.* = .{
                .keys = undefined,
                .children = [_]?*Self{null} ** (MaxKeys + 1),
                .num_keys = 0,
                .is_leaf = true,
                .allocator = allocator,
            };
            return node;
        }

        pub fn deinit(self: *Self) void {
            if (!self.is_leaf) {
                var i: u32 = 0;
                while (i <= self.num_keys) : (i += 1) {
                    if (self.children[i]) |child| {
                        child.deinit();
                    }
                }
            }
            self.allocator.destroy(self);
        }
    };
}

pub fn BTree(comptime T: type, comptime order: u32) type {
    return struct {
        const Self = @This();
        const Node = BTreeNode(T, order);

        root: ?*Node,
        allocator: Allocator,

        pub fn init(allocator: Allocator) Self {
            return .{
                .root = null,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.root) |root| {
                root.deinit();
            }
        }

        pub fn insert(self: *Self, key: T) !void {
            if (self.root == null) {
                self.root = try Node.init(self.allocator);
            }

            const root = self.root.?;
            if (root.num_keys == Node.MaxKeys) {
                // Split root if full
                const new_root = try Node.init(self.allocator);
                self.root = new_root;
                new_root.is_leaf = false;
                new_root.children[0] = root;
                try splitChild(new_root, 0);
                try insertNonFull(new_root, key);
            } else {
                try insertNonFull(root, key);
            }
        }

        fn splitChild(parent: *Node, index: u32) !void {
            const child = parent.children[index].?;
            const new_child = try Node.init(parent.allocator);
            new_child.is_leaf = child.is_leaf;
            new_child.num_keys = Node.MinKeys;

            // Copy right half of keys to new node
            var i: u32 = 0;
            while (i < Node.MinKeys) : (i += 1) {
                new_child.keys[i] = child.keys[i + Node.MinKeys + 1];
            }

            // Copy right half of children if not leaf
            if (!child.is_leaf) {
                i = 0;
                while (i <= Node.MinKeys) : (i += 1) {
                    new_child.children[i] = child.children[i + Node.MinKeys + 1];
                }
            }

            child.num_keys = Node.MinKeys;

            // Make space in parent for new key
            i = parent.num_keys;
            while (i > index) : (i -= 1) {
                parent.children[i + 1] = parent.children[i];
            }
            parent.children[index + 1] = new_child;

            i = parent.num_keys;
            while (i > index) : (i -= 1) {
                parent.keys[i] = parent.keys[i - 1];
            }
            parent.keys[index] = child.keys[Node.MinKeys];
            parent.num_keys += 1;
        }

        fn insertNonFull(node: *Node, key: T) !void {
            var i = node.num_keys;
            if (node.is_leaf) {
                // Find position and insert key
                while (i > 0 and key < node.keys[i - 1]) : (i -= 1) {
                    node.keys[i] = node.keys[i - 1];
                }
                node.keys[i] = key;
                node.num_keys += 1;
            } else {
                // Find child to recurse on
                while (i > 0 and key < node.keys[i - 1]) : (i -= 1) {}
                const child = node.children[i].?;
                if (child.num_keys == Node.MaxKeys) {
                    try splitChild(node, i);
                    if (key > node.keys[i]) {
                        i += 1;
                    }
                }
                try insertNonFull(node.children[i].?, key);
            }
        }

        pub fn search(self: *const Self, key: T) bool {
            if (self.root == null) return false;
            return searchNode(self.root.?, key);
        }

        fn searchNode(node: *const Node, key: T) bool {
            var i: u32 = 0;
            while (i < node.num_keys and key > node.keys[i]) : (i += 1) {}

            if (i < node.num_keys and key == node.keys[i]) {
                return true;
            }

            if (node.is_leaf) {
                return false;
            }

            return searchNode(node.children[i].?, key);
        }
    };
}

test "btree basic operations" {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var tree = BTree(i32, 3).init(allocator);
    defer tree.deinit();

    try tree.insert(10);
    try tree.insert(20);
    try tree.insert(5);
    try tree.insert(15);
    try tree.insert(25);

    try testing.expect(tree.search(10));
    try testing.expect(tree.search(20));
    try testing.expect(tree.search(5));
    try testing.expect(tree.search(15));
    try testing.expect(tree.search(25));
    try testing.expect(!tree.search(30));
}
