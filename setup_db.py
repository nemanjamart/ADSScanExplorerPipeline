from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,relationship
import ADSScanExplorerPipeline.models


engine = create_engine("postgres://scan_explorer:scan_explorer@localhost:5432/scan_explorer_pipeline", echo=False)
conn = engine.connect()
DBSession = sessionmaker(bind=engine)
session = DBSession()

ADSScanExplorerPipeline.models.Base.metadata.drop_all(engine)
ADSScanExplorerPipeline.models.Base.metadata.create_all(engine)
session.commit()