from peewee import *

# db = SqliteDatabase('quantmew.db')
db = MySQLDatabase('quantmew', user='root', password='root', host='127.0.0.1', port=3306)

class BaseModel(Model):
    class Meta:
        database = db

class StockBasic(BaseModel):
    code = CharField(max_length=64, primary_key=True)
    symbol = CharField(max_length=64)
    name = CharField(max_length=64)
    area = CharField(max_length=16, null=True)
    industry = CharField(max_length=16, null=True)
    fullname = CharField(max_length=64)
    enname = CharField(max_length=64)
    market = CharField(max_length=16, null=True)
    exchange = CharField(max_length=16)
    curr_type = CharField(max_length=16)
    list_status = CharField(max_length=8, null=True)
    list_date = DateField(null=True, formats=['%Y%m%d', '%Y-%m-%d'])
    delist_date = DateField(null=True, formats=['%Y%m%d', '%Y-%m-%d'])
    is_hs = CharField(max_length=8, null=True)

class StockCompany(BaseModel):
    code = CharField(max_length=64, primary_key=True)
    exchange = CharField(max_length=64, null=True)
    chairman = CharField(max_length=64, null=True)
    manager = CharField(max_length=64, null=True)
    secretary = CharField(max_length=64, null=True)
    reg_capital = CharField(max_length=64, null=True)
    setup_date = CharField(max_length=64, null=True)
    province = CharField(max_length=64, null=True)
    city = CharField(max_length=64, null=True)
    introduction = CharField(max_length=512, null=True)
    website = CharField(max_length=512, null=True)
    email = CharField(max_length=512, null=True)
    office = CharField(max_length=512, null=True)
    employees = CharField(max_length=512, null=True)
    main_business = CharField(max_length=512, null=True)
    business_scope = CharField(max_length=512, null=True)

class StockManagers(BaseModel):
    code = CharField(max_length=64)
    ann_date = CharField(max_length=64)
    name = CharField(max_length=64)
    gender = CharField(max_length=64, null=True)
    lev = CharField(max_length=64, null=True)
    title = CharField(max_length=64, null=True)
    edu = CharField(max_length=64, null=True)
    national = CharField(max_length=64, null=True)
    birthday = CharField(max_length=64, null=True)
    begin_date = DateField(formats=['%Y%m%d', '%Y-%m-%d'], null=True)
    end_date = DateField(formats=['%Y%m%d', '%Y-%m-%d'], null=True)
    resume = TextField(null=True)
    class Meta:
        primary_key = CompositeKey('code', 'ann_date', 'name')


class TradeCal(BaseModel):
    exchange = CharField(max_length=16)
    cal_date = DateField(formats=['%Y%m%d', '%Y-%m-%d'])
    is_open = IntegerField()
    class Meta:
        primary_key = CompositeKey('exchange', 'cal_date')

class StockDaily(BaseModel):
    code = CharField(max_length=64)
    trade_date = DateField(formats=['%Y%m%d', '%Y-%m-%d'])
    open = DecimalField(max_digits=16, decimal_places=5)
    high = DecimalField(max_digits=16, decimal_places=5)
    low = DecimalField(max_digits=16, decimal_places=5)
    close = DecimalField(max_digits=16, decimal_places=5)
    pre_close = DecimalField(max_digits=16, decimal_places=5)
    change = DecimalField(max_digits=16, decimal_places=5)
    pct_chg = DecimalField(max_digits=16, decimal_places=5)
    vol = DecimalField(max_digits=16, decimal_places=5)
    amount = DecimalField(max_digits=16, decimal_places=5, null=True)
    class Meta:
        primary_key = CompositeKey('code', 'trade_date')

class StockDailyBasic(BaseModel):
    code = CharField(max_length=64)
    trade_date = DateField(formats=['%Y%m%d', '%Y-%m-%d'])
    close = DecimalField(max_digits=16, decimal_places=5)
    turnover_rate = DecimalField(max_digits=16, decimal_places=5, null=True)
    turnover_rate_f = DecimalField(max_digits=16, decimal_places=5, null=True)
    volume_ratio = DecimalField(max_digits=16, decimal_places=5, null=True)
    pe = DecimalField(max_digits=16, decimal_places=5, null=True)
    pe_ttm = DecimalField(max_digits=16, decimal_places=5, null=True)
    pb = DecimalField(max_digits=16, decimal_places=5, null=True)
    ps = DecimalField(max_digits=16, decimal_places=5, null=True)
    ps_ttm = DecimalField(max_digits=16, decimal_places=5, null=True)
    dv_ratio = DecimalField(max_digits=16, decimal_places=5, null=True)
    dv_ttm = DecimalField(max_digits=16, decimal_places=5, null=True)
    total_share = DecimalField(max_digits=16, decimal_places=5, null=True)
    float_share = DecimalField(max_digits=16, decimal_places=5, null=True)
    free_share = DecimalField(max_digits=16, decimal_places=5, null=True)
    total_mv = DecimalField(max_digits=16, decimal_places=5, null=True)
    circ_mv = DecimalField(max_digits=16, decimal_places=5, null=True)

    class Meta:
        primary_key = CompositeKey('code', 'trade_date')

class StockWeekly(BaseModel):
    code = CharField(max_length=64)
    trade_date = DateField(formats=['%Y%m%d', '%Y-%m-%d'])
    open = DecimalField(max_digits=16, decimal_places=5)
    high = DecimalField(max_digits=16, decimal_places=5)
    low = DecimalField(max_digits=16, decimal_places=5)
    close = DecimalField(max_digits=16, decimal_places=5)
    pre_close = DecimalField(max_digits=16, decimal_places=5)
    change = DecimalField(max_digits=16, decimal_places=5)
    pct_chg = DecimalField(max_digits=16, decimal_places=5)
    vol = DecimalField(max_digits=16, decimal_places=5)
    amount = DecimalField(max_digits=16, decimal_places=5, null=True)
    class Meta:
        primary_key = CompositeKey('code', 'trade_date')

class StockHKHold(BaseModel):
    ex_code = CharField(max_length=64)
    code = CharField(max_length=64)
    name = CharField(max_length=64, null=True)
    trade_date = DateField(formats=['%Y%m%d', '%Y-%m-%d'])
    vol = DecimalField(max_digits=16, decimal_places=5, null=True)
    ratio = DecimalField(max_digits=16, decimal_places=5, null=True)
    exchange = CharField(max_length=64)
    class Meta:
        primary_key = CompositeKey('code', 'trade_date')


db.create_tables([
    StockBasic, TradeCal, StockDaily, StockDailyBasic,
    StockWeekly, StockHKHold, StockCompany,
    StockManagers])
