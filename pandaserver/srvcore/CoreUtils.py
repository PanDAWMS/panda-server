import re
import subprocess


# replacement for commands
def commands_get_status_output(com):
    data = ''
    try:
        # not to use check_output for python 2.6
        # data = subprocess.check_output(com, shell=True, universal_newlines=True, stderr=subprocess.STDOUT)
        p = subprocess.Popen(com, shell=True, universal_newlines=True, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        data, unused_err = p.communicate()
        retcode = p.poll()
        if retcode:
            ex = subprocess.CalledProcessError(retcode, com)
            raise ex
        status = 0
    except subprocess.CalledProcessError as ex:
        # commented out for python 2.6
        # data = ex.output
        status = ex.returncode
    if data[-1:] == '\n':
        data = data[:-1]
    return status, data


# extract name from DN
def clean_user_id(id):
    try:
        up = re.compile('/(DC|O|OU|C|L)=[^\/]+')
        username = up.sub('', id)
        up2 = re.compile('/CN=[0-9]+')
        username = up2.sub('', username)
        up3 = re.compile(' [0-9]+')
        username = up3.sub('', username)
        up4 = re.compile('_[0-9]+')
        username = up4.sub('', username)
        username = username.replace('/CN=proxy', '')
        username = username.replace('/CN=limited proxy', '')
        username = username.replace('limited proxy', '')
        username = re.sub('/CN=Robot:[^/]+', '', username)
        pat = re.compile('.*/CN=([^\/]+)/CN=([^\/]+)')
        mat = pat.match(username)
        if mat:
            username = mat.group(2)
        else:
            username = username.replace('/CN=', '')
        if username.lower().find('/email') > 0:
            username = username[:username.lower().find('/email')]
        pat = re.compile('.*(limited.*proxy).*')
        mat = pat.match(username)
        if mat:
            username = mat.group(1)
        username = username.replace('(', '')
        username = username.replace(')', '')
        username = username.replace("'", '')
        return username
    except Exception:
        return id
