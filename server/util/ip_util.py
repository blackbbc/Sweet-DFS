import netifaces as ni

def find_ip(interface):
    return ni.ifaddresses(interface)[ni.AF_INET][0]['addr']
