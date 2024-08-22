from sqlalchemy import Column, Integer, Column, Float
from app.database.connect import Base 

class MainInput(Base):
    __tablename__ = "main_input"

    id = Column(Integer, primary_key=True, index=True)
    sum_assured = Column(Integer)
    num_policies = Column(Integer)
    prem_rate_per1000 = Column(Float)
    policy_fees = Column(Integer)
    policy_init_comm = Column(Float)
    policy_yearly_comm = Column(Float)
    acq_direct_expenses = Column(Integer)
    acq_indirect_expense = Column(Integer)
    main_direct_expenses = Column(Integer)
    main_indirect_expenses = Column(Integer)
    total_years = Column(Integer)
    discount_rate = Column(Float)
    asset_ret_rate = Column(Float)
    csm_ret_rate = Column(Float)
    risk_adjst_rate = Column(Float)
    mortality = Column(Float)
    lapse = Column(Float)