from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float



Base = declarative_base()

class CAR(Base):

    __tablename__ = 'car'

    car_hash = Column(String, primary_key=True)    
    
    fuel = Column(String)
    title = Column(String)
    brand = Column(String)
    model = Column(String)
    motor = Column(String)
    valves = Column(String)
    version = Column(String)
    traction = Column(String)
    category = Column(String)
    transmission = Column(String)
    cylinder_pos = Column(String)
    cylinder_power = Column(String)

    year_model = Column(Date)
    year_fabrication = Column(Date)
    
    ports = Column(Integer)
    armored = Column(Integer)

class DATA(Base):

    __tablename__ = 'data'

    id = Column(Integer, primary_key=True)    
    car_hash = Column(String, ForeignKey('car.car_hash'))    
    
    date = Column(Date)

    price = Column(Float)
    fipe_percent = Column(Float)
    
    licence = Column(Integer)
    odometer = Column(Integer)
    warranty = Column(Integer)
    alienated = Column(Integer)
    financing = Column(Integer)
    ipva_paid = Column(Integer)
    good_deal = Column(Integer)
    collection = Column(Integer)
    first_owner = Column(Integer)
    car_delivery = Column(Integer)
    vip_autopago = Column(Integer)
    dealer_score = Column(Integer)
    exceeded_plan = Column(Integer)
    accept_exchange = Column(Integer)
    troca_com_troco = Column(Integer)
    contains_leilao = Column(Integer)
    scheduled_review = Column(Integer)
    dealership_review = Column(Integer)
    disability_driver = Column(Integer)

    city = Column(String)
    color = Column(String)
    state = Column(String)
    hot_deal = Column(String)
    seller_type = Column(String)
    marketplace = Column(String)
    long_comment = Column(String)
    
    

