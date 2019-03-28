from utils import get_env_variable
 
DB_USER = get_env_variable("DATABASE_USER")
DB_PASSWORD = get_env_variable("DATABASE_PASSWORD")
DB_NAME = get_env_variable("DATABASE_NAME")
DB_HOST = get_env_variable("DATABASE_HOST")
DB_PORT = get_env_variable("DATABASE_PORT")
HTTP_SCORING_AUTH = get_env_variable("HEADERS")
