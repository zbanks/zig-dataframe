const std = @import("std");
const ArrayList = std.ArrayList;
const TypeInfo = std.builtin.TypeInfo;
const meta = std.meta;

const ParseError = error { ParseError } || anyerror;

fn parse(comptime Type: type, buf: []const u8) ParseError!Type {
    const typeInfo = @typeInfo(Type);
    const TypeId = std.builtin.TypeId;
    return switch (typeInfo) {
        .Int => std.fmt.parseInt(Type, buf, 10),
        .Float => std.fmt.parseFloat(Type, buf),
        .Enum => |en| {
            inline for (en.fields) |field| {
                if (std.mem.eql(u8, field.name, buf)) {
                    return @intToEnum(Type, field.value);
                }
            }
            return ParseError.ParseError;
        },
        .Bool => std.mem.eql(u8, "true", buf),
        else => {
            @compileError("parse not supported for " ++ @typeName(Type));
            unreachable;
        },
    };
}

fn initFromPair(comptime T: type, a: anytype, b: anytype) T {
    var t = std.mem.zeroInit(T, a);
    inline for (@typeInfo(@TypeOf(b)).Struct.fields) |field| {
        @field(t, field.name) = @field(b, field.name);
    }
    return t;
}

fn initSubset(comptime T: type, a: anytype) T {
    var t = std.mem.zeroInit(T, .{});
    inline for (@typeInfo(T).Struct.fields) |field| {
        if (@hasField(@TypeOf(a), field.name)) {
            @field(t, field.name) = @field(a, field.name);
        }
    }
    return t;
}

fn fieldByName(comptime T: type, comptime name: []const u8) TypeInfo.StructField {
    const index = meta.fieldIndex(T, name) orelse unreachable;
    return meta.fields(T)[index];
}

fn DataFrame(comptime RowLabel: type, comptime ColumnLabel: type) comptime type {
    if (@typeInfo(RowLabel) != .Struct) {
        @compileError("Dataframe Row type must be a struct, got " ++ @typeName(RowLabel));
    }
    if (@typeInfo(ColumnLabel) != .Struct) {
        @compileError("Dataframe Column type must be a struct, got " ++ @typeName(ColumnLabel));
    }
    const rowFields = meta.fields(RowLabel);
    const columnFields = meta.fields(ColumnLabel);
    inline for (columnFields) |field| {
        if (@hasField(RowLabel, field.name)) {
            @compileError("Dataframe Row & Column types must have disjoint field names; they both have \"" ++ field.name ++ "\"");
        }
    }
    const dataFields = rowFields ++ columnFields;

    return struct {
        data: DataArray,

        const Self = @This();
        const RowType = RowLabel;
        const ColumnType = ColumnLabel;
        const DataType = @Type(TypeInfo{ .Struct = .{
            .layout = .Auto,
            .fields = dataFields,
            .decls = &[0]TypeInfo.Declaration{},
            .is_tuple = false,
        } });
        const DataArray = ArrayList(DataType);

        pub fn initEmpty(allocator: *std.mem.Allocator) !Self {
            var self = Self{
                .data = DataArray.init(allocator),
            };

            return self;
        }

        pub fn deinit(self: Self) void {
            self.data.deinit();
        }

        const FileError = error{ParseError} || anyerror;

        pub fn initFromFile(filename: []const u8, allocator: *std.mem.Allocator) FileError!Self {
            var self = try Self.initEmpty(allocator);
            errdefer self.deinit();

            var file = try std.fs.cwd().openFile(filename, .{});
            defer file.close();

            var reader = std.io.bufferedReader(file.reader()).reader();

            const max_length: usize = 1024;
            {
                const header = (try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', max_length)) orelse return FileError.ParseError;
                defer allocator.free(header);
                var headerIter = std.mem.tokenize(header, ",");
                inline for (columnFields) |field| {
                    const col = headerIter.next();
                    std.debug.assert(col != null);
                    std.debug.assert(std.mem.eql(u8, col.?, field.name));
                }
                std.debug.assert(headerIter.rest().len == 0);
            }

            while (try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', max_length)) |line| {
                defer allocator.free(line);
                var lineIter = std.mem.tokenize(line, ",");
                var entry = std.mem.zeroInit(ColumnLabel, .{});
                inline for (columnFields) |field| {
                    const cell = lineIter.next() orelse return FileError.ParseError;
                    const value = try parse(field.field_type, cell);
                    @field(entry, field.name) = value;
                }
                var index = std.mem.zeroInit(RowLabel, .{});
                try self.append(index, entry);
            }

            return self;
        }

        pub fn append(self: *Self, index: RowLabel, data: ColumnLabel) !void {
            var d = initFromPair(DataType, index, data);
            try self.data.append(d);
        }

        pub fn sort(self: *Self, comptime column: [] const u8) void {
            const sortFn = struct {
                fn inner(context: void, a: DataType, b: DataType) bool {
                    return @field(a, column) < @field(b, column);
                }
            }.inner;
            std.sort.sort(DataType, self.data.items, {}, sortFn);
        }

        fn ReindexType(comptime labels: anytype) comptime type {
            const labelsInfo = @typeInfo(@TypeOf(labels));
            if (labelsInfo != .Struct or !labelsInfo.Struct.is_tuple) {
                @compileError("Expected `labels` parameter to be a tuple, got " ++ @typeName(@TypeOf(labels)));
            }
            const labelFields = labelsInfo.Struct.fields;
            inline for (labelFields) |field| {
                const label = @field(labels, field.name);
                if (!@hasField(DataType, label)) {
                    @compileError("DataFrame does not have label named \"" ++ label ++ "\"");
                }
            }

            comptime var newRowInfo = @typeInfo(RowLabel);
            comptime var newColumnInfo = @typeInfo(ColumnLabel);
            newRowInfo.Struct.fields = &[0]TypeInfo.StructField{};
            newColumnInfo.Struct.fields = &[0]TypeInfo.StructField{};

            // Partition the fields into a new RowType & ColumnType
            // If the field is in `labels` then it goes in RowType
            inline for (dataFields) |field| {
                comptime var inLabels = false;
                inline for (labelFields) |labelField| {
                    const label = @field(labels, labelField.name);
                    if (std.mem.eql(u8, label, field.name)) {
                        inLabels = true;
                        break;
                    }
                }
                if (inLabels) {
                    newRowInfo.Struct.fields = newRowInfo.Struct.fields ++ [1]TypeInfo.StructField{field};
                } else {
                    newColumnInfo.Struct.fields = newColumnInfo.Struct.fields ++ [1]TypeInfo.StructField{field};
                }
            }

            const NewRowType = @Type(newRowInfo);
            const NewColumnType = @Type(newColumnInfo);
            return DataFrame(NewRowType, NewColumnType);
        }

        pub fn reindex(self: Self, comptime labels: anytype) !ReindexType(labels) {
            const NewType = ReindexType(labels);
            var new = try NewType.initEmpty(self.data.allocator);
            for (self.data.items) |entry| {
                try new.data.append(std.mem.zeroInit(NewType.DataType, entry));
            }
            return new;
        }

        fn PivotType(comptime index: []const u8, comptime columns: []const u8, comptime value: []const u8) comptime type {
            if (!@hasField(DataType, index)) {
                @compileError("pivot index \"" ++ index ++ "\" does not exist");
            }
            if (!@hasField(DataType, columns)) {
                @compileError("pivot column \"" ++ columns ++ "\" does not exist");
            }
            if (!@hasField(DataType, value)) {
                @compileError("pivot value \"" ++ value ++ "\" does not exist");
            }
            if (std.mem.eql(u8, index, columns) or std.mem.eql(u8, index, value) or std.mem.eql(u8, columns, value)) {
                @compileError("pivot index, column, and value must all be unique");
            }

            comptime var newRowInfo = @typeInfo(RowLabel);
            newRowInfo.Struct.fields = &[1]TypeInfo.StructField{ fieldByName(DataType, index) };
            const NewRowType = @Type(newRowInfo);

            const ColumnFieldType = fieldByName(DataType, columns).field_type;
            if (@typeInfo(ColumnFieldType) != .Enum) {
                @compileError("column must be an enum: " ++ columns ++ ", " ++ @typeName(NewColumnType));
            }
            const ValueType = fieldByName(DataType, value).field_type;
            const NewColumnType = std.enums.EnumFieldStruct(ColumnFieldType, ?ValueType, null);

            return DataFrame(NewRowType, NewColumnType);
        }

        pub fn pivot(self: Self, comptime index: []const u8, comptime columns: []const u8, comptime value: []const u8) !PivotType(index, columns, value) {
            const NewType = PivotType(index, columns, value);
            const ColumnEnum = fieldByName(DataType, columns).field_type;

            var new = try NewType.initEmpty(self.data.allocator);

            for (self.data.items) |entry, idx| {
                const newRow = idx == 0 or @field(entry, index) != @field(self.data.items[idx-1], index);
                var d = if (newRow) try new.data.addOne() else &new.data.items[new.data.items.len-1];
                if (newRow) {
                    d.* = initSubset(NewType.DataType, entry);
                }

                inline for (@typeInfo(NewType.ColumnType).Struct.fields) |field| {
                    if (@field(entry, columns) == std.meta.stringToEnum(ColumnEnum, field.name)) {
                        @field(d.*, field.name) = @field(entry, value);
                    }
                }
            }
            return new;
        }


        pub fn format(
            self: Self,
            comptime fmt: []const u8,
            options: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            try writer.print("DataFrame [ ", .{});
            inline for (rowFields) |field| {
                try writer.print("{s}:{} ", .{ field.name, field.field_type });
            }
            try writer.print("] x [ ", .{});
            inline for (columnFields) |field| {
                try writer.print("{s}:{} ", .{ field.name, field.field_type });
            }
            try writer.print("] ({} rows) {{", .{ self.data.items.len });

            if (self.data.items.len != 0) {
                try writer.print("\n", .{});
            }
            for (self.data.items) |row, index| {
                const r = initSubset(RowType, row);
                const c = initSubset(ColumnType, row);
                try writer.print("    {}: {} = {},\n", .{ index, r, c });
            }
            try writer.print("}}", .{});
        }
    };
}

test {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const C = enum { foo, bar, baz, };
    const Entry = struct {
        a: usize,
        b: f64,
        c: C,
        d: bool,
    };
    const Label = struct {};

    const DF = DataFrame(Label, Entry);
    var df = try DF.initFromFile("abc.csv", &gpa.allocator);
    defer df.deinit();

    try df.append(.{}, .{ .a = 4, .b = 2.0, .c = .foo, .d = true });
    try df.append(.{}, .{ .a = 4, .b = 4.0, .c = .bar, .d = true });
    try df.append(.{}, .{ .a = 4, .b = 6.0, .c = .baz, .d = false });
    try df.append(.{}, .{ .a = 1, .b = 8.0, .c = .baz, .d = true });

    df.sort("a");

    std.debug.print("loaded abc.csv:\n{}\n", .{df});

    var df2 = try df.reindex(.{"b"});
    defer df2.deinit();
    std.debug.print("after reindex:\n{}\n", .{df2});

    var df3 = try df.pivot("a", "c", "b");
    defer df3.deinit();
    std.debug.print("after pivot:\n{}\n", .{df3});
}
