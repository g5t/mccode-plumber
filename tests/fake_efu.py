#!/usr/bin/env python

"""
A fake EFU server for testing purposes. Arguments needed for a real EFU are accepted,
but only the command port is utilized.

Usage:
------
Extra carriage returns inserted to keep client and server timelines in sync vertically

(client)                               |  (server)
$                                      |  $ python fake_efu.py --cmdport 10123
$ echo hello | netcat localhost 10123  |  1. connection from ('127.0.0.1', xxxxx)
hello                                  |  ('127.0.0.1', xxxxx) says: hello
$                                      |
$ echo EXIT | netcat localhost 10123   |  1. connection from ('127.0.0.1', YYYYY)
<OK>$                                  |  ('127.0.0.1', YYYYY) says: EXIT
$                                      |
$                                      |  $
$                                      |  $ python fake_efu.py --cmdport 10456
$ netcat localhost 10456               |  1. connection from ('127.0.0.1', ZZZZZ)
Hello?                                 |  ('127.0.0.1', ZZZZZ) says: Hello
Hello?                                 |
Goodbye                                |  ('127.0.0.1', ZZZZZ) says: Goodbye
Goodbye                                |
EXIT                                   |
$ echo EXIT | netcat localhost 10123   |  ('127.0.0.1', ZZZZZ) says: EXIT
<OK>$                                  |
$                                      |  $

(Note that 'EXIT' within interactive netcat is a command and is not sent.)
"""

def main():
    import sys
    from argparse import ArgumentParser
    parser = ArgumentParser(prog='fake_efu', description='Fake Efu commands')
    parser.add_argument('--cmdport', type=int)
    args, unknown = parser.parse_known_args()
    if unknown:
        print(f"Ignoring unknown args: {unknown}", file=sys.stderr)
    serve(args.cmdport)


def serve(p):
    import socket
    l = []
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Since we're undoubtedly calling this after an ephemeral_port_reserve.reserve() call
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', p))
    s.listen(1)
    while 1:
        (c, a) = s.accept()
        l.append(c)
        print(f'{len(l)}: connection from {a}')
        received = str(c.recv(1024), 'utf-8')
        print(f'{a} says: {received}')
        if received == 'EXIT\n':
            c.sendall(bytes('<OK>', 'utf-8'))
            exit(0)
        else:
            c.sendall(bytes(received, 'utf-8'))
        c.close()


if __name__ == '__main__':
    main()
else:
    print(f'Imported? {__name__}')
