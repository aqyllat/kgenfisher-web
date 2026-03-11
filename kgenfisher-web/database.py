import os
import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from passlib.context import CryptContext
import jwt

# ── Database Setup ──

# On Railway, use /tmp/ because the app directory may be read-only
if os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_PROJECT_ID"):
    DB_FILE = "/tmp/kgenfisher.db"
else:
    DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kgenfisher.db")

DATABASE_URL = f"sqlite:///{DB_FILE}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ── Auth Setup ──

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
SECRET_KEY = "kgenfisher_super_secret_key_change_in_production"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # 7 days

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# ── Models ──

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    accounts = relationship("KGenAccount", back_populates="owner", cascade="all, delete-orphan")

class KGenAccount(Base):
    __tablename__ = "kgen_accounts"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    bearer_token = Column(Text, nullable=False)
    refresh_token = Column(Text, nullable=True)
    added_at = Column(DateTime, default=datetime.datetime.utcnow)
    username = Column(String(100), nullable=True)
    points = Column(Integer, default=0)
    is_valid = Column(Integer, default=1)
    owner = relationship("User", back_populates="accounts")

# Create tables
Base.metadata.create_all(bind=engine)

# ── Dependency ──
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
