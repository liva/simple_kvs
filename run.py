#!/usr/bin/env python
from scripts import *
from datetime import datetime
import shutil

conf = {
    "ve": True,
    #"gdb": True,
}
def main():
    env = get_env('output', **conf)
    env.write("now: {}\n".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
    env.build("-Wall -g3 -O2 --std=c++11 -o test/a.out test/optional.cc test/status.cc test/main.cc test/simple_io.cc test/iterator.cc test/slice.cc test/slice_container.cc")
    env.run_command("./test/a.out")
    #shell.call("docker run --rm -it -v $PWD:$PWD -w $PWD unvme:ve /opt/nec/nosupport/llvm-ve/bin/clang++ -g3 -O2 --target=ve-linux -static --std=c++11 -o main main.cc -L/opt/nec/nosupport/llvm-ve/lib/clang/10.0.0/lib/linux -lclang_rt.builtins-ve  -lpthread -lm -lc ")
    #shell.check_call("./main")

    shell.call("docker rm -f simple_kvs")
    shell.check_call("docker run -d --name simple_kvs -it -v $PWD:$PWD -w $PWD leveldb:ve sh")
    shell.check_call("docker exec -it simple_kvs cp -r *.h /opt/nec/ve/include")
    shell.check_call("docker exec -it simple_kvs cp -r *.h /opt/nec/ve/ex_include")
    shell.check_call("docker exec -it simple_kvs cp -r utils /opt/nec/ve/include")
    shell.check_call("docker exec -it simple_kvs cp -r utils /opt/nec/ve/ex_include")
    shell.check_call("docker commit simple_kvs simple_kvs:ve")
    shell.check_call("docker rm -f simple_kvs")

if __name__ == "__main__":
    main()