from fluxi_engine.observability import configure_logging
from fluxi_engine.server import create_app


configure_logging("fluxi-server")
app = create_app()
