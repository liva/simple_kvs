#!/usr/bin/env python
from scripts import *
from datetime import datetime
import shutil

class Test:
    def __init__(self, env, source_files):
        self.env = env
        self.source_files = source_files
        self.storage = UioNvme()
    def build_and_run(self, extra_option=''):
        self.env.build("-Wall -g3 -O2 --std=c++11 {} -I. -o test/a.out {} -lvefs -lunvme -lsysve".format(extra_option, self.source_files))
        nvme.cleanup(UioNvme())
        self.storage.setup()
        self.env.run_command_with_sudo("./test/a.out")
        self.storage.cleanup()

conf = {
    "ve": True,
    #"gdb": True,
}
def main():
    env = get_env('output', **conf)
    env.write("now: {}\n".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
    #Test(env, "test/optional.cc").build_and_run()
    #Test(env, "test/status.cc").build_and_run()
    #Test(env, "test/slice.cc").build_and_run()
    #Test(env, "test/block_storage.cc").build_and_run()
    #Test(env, "test/char_storage.cc").build_and_run()
    #Test(env, "test/simple_io.cc").build_and_run()
    #Test(env, "test/iterator.cc").build_and_run()
    #Test(env, "test/persistence.cc").build_and_run()
    Test(env, "test/performance_evaluation.cc").build_and_run('-DNDEBUG')

    #shell.call("docker run --rm -it -v $PWD:$PWD -w $PWD unvme:ve /opt/nec/nosupport/llvm-ve/bin/clang++ -g3 -O2 --target=ve-linux -static --std=c++11 -o main main.cc -L/opt/nec/nosupport/llvm-ve/lib/clang/10.0.0/lib/linux -lclang_rt.builtins-ve  -lpthread -lm -lc ")
    #shell.check_call("./main")

    shell.call("docker rm -f simple_kvs")
    shell.check_call("docker run -d --name simple_kvs -it -v $PWD:$PWD -w $PWD leveldb:ve sh")
    shell.call("docker exec -it simple_kvs cp -r *.h /opt/nec/ve/include")
    shell.call("docker exec -it simple_kvs cp -r *.h /opt/nec/ve/ex_include")
    shell.check_call("docker exec -it simple_kvs cp -r utils block_storage char_storage kvs /opt/nec/ve/include")
    shell.check_call("docker exec -it simple_kvs cp -r utils block_storage char_storage kvs /opt/nec/ve/ex_include")
    shell.check_call("docker commit simple_kvs simple_kvs:ve")
    shell.check_call("docker rm -f simple_kvs")

if __name__ == "__main__":
    main()