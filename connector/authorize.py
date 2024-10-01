from configparser import ConfigParser, MissingSectionHeaderError

def getConnectionString(implementation: str, section: str, filename: str) -> str:
    config = ConfigParser()
    try:
        config.read(filename)
    except FileNotFoundError:
        print('Configuration File NOT FOUND')
    except MissingSectionHeaderError:
        print(f'{filename} Format Error')

    connectionString = ''

    if implementation.upper() == 'ORACLE':

        for option in config.options(section):
            connectionString += config.get(section, option)
            if option in ('user', 'port'):
                connectionString += f'/'
            elif option == 'host':
                connectionString += ':'
            elif option == 'password':
                connectionString += '@'
            else:
                pass

    return connectionString

def getConnectionStringEngine(implementation: str, section: str, filename: str) -> str:
    config = ConfigParser()
    try:
        config.read(filename)
    except FileNotFoundError:
        print('Configuration File NOT FOUND')
    except MissingSectionHeaderError:
        print(f'{filename} Format Error')


    if implementation.upper() == 'ORACLE':
        connectionString = r'oracle+oracledb://'

        for option in config.options(section):
            if option == 'user':
                connectionString += f'{config.get(section, option)}:'
            elif option == 'host':
                connectionString += config.get(section, option)
            elif option == 'password':
                connectionString += f'{config.get(section, option)}@'
            elif option == 'dbname':
                connectionString += f'/?service_name={config.get(section, option).upper()}'
            else:
                pass

    return connectionString

def getAPIKey(filename: str, section: str) -> str:
    config = ConfigParser()
    try:
        config.read(filename)
    except FileNotFoundError:
        print('Configuration File NOT FOUND')
    except MissingSectionHeaderError:
        print(f'{filename} Format Error')

    option = config.options(section)[0]

    return config.get(section, option)