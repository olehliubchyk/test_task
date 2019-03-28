import os
 
def get_env_variable(var_name, delimiter=None):
    """
   Get the environment variable or return exception
   :param var_name:
   :param delimiter:
   :return:
   """
    try:
        env_variable = os.environ[var_name]
        if delimiter:
            env_variable = env_variable.split(delimiter)
        return env_variable
    except KeyError:
        error_msg = "Set the {} environment variable".format(var_name)
    raise Exception(error_msg)
