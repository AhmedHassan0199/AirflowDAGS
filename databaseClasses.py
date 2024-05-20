from sqlalchemy import create_engine,ForeignKey,Column,String,Integer,CHAR,engine,MetaData,DateTime,Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()
#Dim Product
class Product(Base):
    __tablename__ = "DimProduct"
    product_id = Column("product_id",Integer,primary_key = True,autoincrement=True)
    item_id = Column("item_id",String)
    sku = Column("sku",String)
    category = Column("category",String)
    price = Column("price",String)
    
    
    def map(column):
        mapper = {
            'item_id': "item_id",
            'sku': "sku",
            'category': "category",
            'price': "price"
        }
        return mapper[column]
    
    def __init__(self, item_id,sku,category,price):
       self.item_id = item_id
       self.sku = sku
       self.category = category
       self.price = price
    
    def getDataTypes():
        return dict()

    def __repr__(self):
        return f"{self.item_id} {self.sku} {self.category} {self.price}"
    
    def __eq__(self, other):
        return self.item_id==other.item_id and self.sku==other.sku and self.category == other.category and self.price == other.price
    
    def __hash__(self):
        return hash(('item_id', self.item_id,'sku', self.sku,'category',self.category,'price',self.price))
    
    def to_dict(self):
        return {
            'item_id': self.item_id,
            'sku': self.sku,
            'category': self.category,
            'price': self.price
        }
################

#Dim Date
class Date(Base):
    __tablename__ = "DimDate"
    date_id = Column("date_id",Integer,primary_key = True,autoincrement=True)
    order_date = Column("order_date",DateTime)
    year = Column("year",String)
    month = Column("month",String)
    
    
    def map(column):
        mapper = {
            'order_date': "order_date",
            'year': "year",
            'month': "month",
        }
        return mapper[column]

    def getDataTypes():
        return {'order_date':datetime.date}
    
    def __init__(self, order_date,year,month):
       self.order_date = order_date
       self.year = year
       self.month = month
    
    def __repr__(self):
        return f"{self.order_date} {self.year} {self.month}"
    
    def __eq__(self, other):
        return self.order_date==other.order_date and self.year==other.year and self.month == other.month
    
    def __hash__(self):
        return hash(('order_date', self.order_date,'year', self.year,'month',self.month))
    
    def to_dict(self):
        return {
            'order_date': self.order_date,
            'year': self.year,
            'month': self.month,
        }
################

#Dim Customer
class Customer(Base):
    __tablename__ = "DimCustomer"
    customer_key = Column("customer_key",Integer,primary_key = True,autoincrement=True)
    customer_id = Column("cust_id",Integer)
    name_prefix = Column("Name Prefix",String)
    first_name = Column("First Name",String)
    middle_initial = Column("Middle Initial",String)
    last_name = Column("Last Name",String)
    gender = Column("Gender",String)
    age = Column("age",Integer)
    full_name = Column("full_name",String)
    email = Column("E Mail",String)
    sign_in_date = Column("Sign in date",DateTime)
    phone_number = Column("Phone No.",String)
    user_name = Column("User Name",String)
    

    def getDataTypes():
        return {'customer_id':int,'sign_in_date':datetime.date,'age':int}
    
    
    def map(column):
        mapper  = {
            'cust_id': "customer_id",
            'Name Prefix': "name_prefix",
            'First Name': "first_name",
            'Middle Initial': "middle_initial",
            'Last Name': "last_name",
            'Gender': "gender",
            'age': "age",
            'full_name': "full_name",
            'E Mail': "email",
            'Sign in date': "sign_in_date",
            'Phone No.': "phone_number",
            'User Name':"user_name"
        }
        return mapper[column]
        

    def __init__(self, customer_id, name_prefix, first_name, middle_initial, last_name, gender, age, full_name, email, sign_in_date, phone_number, user_name):
        self.customer_id = customer_id
        self.name_prefix = name_prefix
        self.first_name = first_name
        self.middle_initial = middle_initial
        self.last_name = last_name
        self.gender = gender
        self.age = age
        self.full_name = full_name
        self.email = email
        self.sign_in_date = sign_in_date
        self.phone_number = phone_number
        self.user_name = user_name
    
    def __repr__(self):
        return f"{self.customer_id} {self.user_name}"

    def __eq__(self, other):
        return (self.customer_id == other.customer_id and self.user_name == other.user_name)
    
    def __hash__(self):
        return hash(('customer_id', self.customer_id,'user_name', self.user_name))
    
    def to_dict(self):
        return {
            'cust_id': self.customer_id,
            'Name Prefix': self.name_prefix,
            'First Name': self.first_name,
            'Middle Initial': self.middle_initial,
            'Last Name': self.last_name,
            'Gender': self.gender,
            'age': self.age,
            'full_name': self.full_name,
            'E Mail': self.email,
            'Sign in date': self.sign_in_date,
            'Phone No.': self.phone_number,
            'User Name': self.user_name
        }
    
#Dim Order Details
class OrderDetails(Base):
    __tablename__ = "DimOrderDetails"
    order_details_key = Column("order_details_key",Integer,primary_key = True,autoincrement=True)
    order_key = Column("order_key",Integer)
    order_id = Column("order_id",String)
    status = Column("status",String)
    qty_ordered = Column("qty_ordered",String)
    value = Column("value",Float)
    discount_amount = Column("discount_amount",Float)
    payment_method = Column("payment_method",String)
    bi_st = Column("bi_st",String)
    ref_num = Column("ref_num",String)
    Discount_Percent = Column("Discount_Percent",Float)
    

    def getDataTypes():
        return {'order_key':int,'value':float,'discount_amount':float,'Discount_Percent':float}
    
    def map(column):
        mapper = {
            'order_key': 'order_key',
            'order_id': 'order_id',
            'status': 'status',
            'qty_ordered': 'qty_ordered',
            'value': 'value',
            'discount_amount': 'discount_amount',
            'payment_method': 'payment_method',
            'bi_st': 'bi_st',
            'ref_num': 'ref_num',
            'Discount_Percent':'Discount_Percent'
        }
        return mapper[column]

    def __init__(self, order_key, order_id, status, qty_ordered, value, discount_amount, payment_method, bi_st, ref_num, Discount_Percent):
        self.order_key = order_key
        self.order_id = order_id
        self.status = status
        self.qty_ordered = qty_ordered
        self.value = value
        self.discount_amount = discount_amount
        self.payment_method = payment_method
        self.bi_st = bi_st
        self.ref_num = ref_num
        self.Discount_Percent = Discount_Percent
    
    def __repr__(self):
        return f"{self.order_key}"

    def __eq__(self, other):
        return (self.order_key == other.order_key and self.order_id == other.order_id)
    
    def __hash__(self):
        return hash(('order_key', self.order_key , 'order_id' , self.order_id))

    def to_dict(self):
        return {
            'order_key': self.order_key,
            'order_id': self.order_id,
            'status': self.status,
            'qty_ordered': self.qty_ordered,
            'value': self.value,
            'discount_amount': self.discount_amount,
            'payment_method': self.payment_method,
            'bi_st': self.bi_st,
            'ref_num': self.ref_num,
            'Discount_Percent': self.Discount_Percent
        }

#Fact Order
class Order(Base):
    __tablename__ = "FactOrder"
    order_id = Column("order_id", Integer, primary_key=True, autoincrement=True)
    product_id = Column("product_id", Integer, ForeignKey("DimProduct.product_id"))
    date_id = Column("date_id", Integer, ForeignKey("DimDate.date_id"))
    customer_key = Column("customer_key", Integer, ForeignKey("DimCustomer.customer_key"))
    order_details_key = Column("order_details_key", Integer, ForeignKey("DimOrderDetails.order_details_key"))
    total = Column("total",Float)

    # Define relationships
    product = relationship("Product")
    date = relationship("Date")
    customer = relationship("Customer")
    order_details = relationship("OrderDetails")

    def getDataTypes():
        return {
            'product_id': int,
            'date_id': int,
            'customer_key': int,
            'order_details_key': int,
            'total':float
        }
    
    def map(column):
        mapper = {
            'product_id': 'product_id',
            'date_id': 'date_id',
            'customer_key': 'customer_key',
            'order_details_key': 'order_details_key',
            'total':'total'
        }
        return mapper[column]

    def __init__(self, product_id, date_id, customer_key, order_details_key,total):
        self.product_id = product_id
        self.date_id = date_id
        self.customer_key = customer_key
        self.order_details_key = order_details_key
        self.total = total
    
    def __repr__(self):
        return f"{self.product_id} {self.date_id} {self.customer_key} {self.order_details_key} {self.total}"

    def __eq__(self, other):
        return (self.product_id == other.product_id and self.date_id == other.date_id and self.customer_key == other.customer_key and self.order_details_key == other.order_details_key and self.total == other.total)

    def __hash__(self):
        return hash(('product_id', self.product_id,'date_id', self.date_id,'customer_key', self.customer_key,'order_details_key', self.order_details_key,'total', self.total,))

    def to_dict(self):
        return {
            'product_id': self.product_id,
            'date_id': self.date_id,
            'customer_key': self.customer_key,
            'order_details_key': self.order_details_key,
            'total' : self.total
        }


