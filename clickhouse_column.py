from clickhouse_sqlalchemy import types


class ClickhouseColumn:
    def __init__(self, *args, **kwargs):
        src_name = kwargs.pop("src_name", None)
        dst_name = kwargs.pop("dst_name", None)
        type_ = kwargs.pop("type", None)
        default = kwargs.pop("default", None)
        args = list(args)
        if args:
            if src_name is None:
                src_name = args.pop(0)
        if args:
            if dst_name is None:
                dst_name = args.pop(0)
        if args:
            if type_ is None:
                type_ = args.pop(0)
        if args:
            if default is None:
                default = args.pop(0)

        # default value for each type
        default_tmp = ""
        if issubclass(type_, types.common.String):
            default_tmp = ""
        if issubclass(type_, types.Int):
            default_tmp = 0
        if issubclass(type_, types.Float):
            default_tmp = 0
        if issubclass(type_, types.Boolean):
            default_tmp = False
        if issubclass(type_, types.DateTime):
            default_tmp = 0

        self.src_name = src_name
        self.dst_name = dst_name if dst_name is not None else src_name
        self.type = type_
        self.default = default if default is not None else default_tmp

    def get_value(self, value):
        res = None
        if value is None:
            return self.default
        try:
            if issubclass(self.type, types.common.String):
                res = str(value)
            elif issubclass(self.type, types.Int):
                res = int(value)
            elif issubclass(self.type, types.Float):
                res = float(value)
            elif issubclass(self.type, types.Boolean):
                res = bool(value)
            elif issubclass(self.type, types.DateTime):
                res = value
        except ValueError as e:
            return self.default

        return res
