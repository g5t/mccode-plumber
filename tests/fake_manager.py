#!/usr/bin/env python

import socket
def sender(port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.settimeout(1.0)
            sock.connect(('localhost', port))
            sock.sendall(bytes(message, 'utf-8'))
            recv = str(sock.recv(1024), "utf-8")
        except TimeoutError:
            print("Timeout!")
            exit(1)
        except ConnectionRefusedError:
            print('Connection Refused')
            exit(1)
    print(recv)

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('p', type=int)
    parser.add_argument('message', type=str, default='message')
    args = parser.parse_args()
    sender(args.p, args.message)



