# consmgr
Console Server Manager for RunC is an experemental project to manage console-sockets for RunC containers.

The consmgr binary can be used as a server as well as a client.

```
» ./consmgr --help
NAME:
   consmgr - A server/client tool to manage runc console sockets

USAGE:
   consmgr [global options] command [command options] [arguments...]

VERSION:
   0.0.1

COMMANDS:
     server   Starts console socket management server
     list     Get a list of available socket ids
     attach   Attaches client to a specified container output
     socket   Create a new socket
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h     show help
   --version, -v  print the version
```

Each subcommand has own parameters check.
