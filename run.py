#!/usr/bin/env python3
from scripts import *
import shutil

class Test:
    def __init__(self, env, source_files, run_cnt = 1):
        self.env = env
        self.source_files = source_files
        self.storage = UioNvme()
        self.run_cnt = run_cnt
    def build_and_run(self, extra_option=''):
        self.env.build("-Wall -g3 -O2 --std=c++11 {} -I. -o test/a.out {} -lvefs -lunvme -lsysve".format(extra_option, self.source_files))
        for i in range(self.run_cnt):
            self.storage.blkdiscard()
            self.storage.setup()
            self.env.run_command_with_sudo("./test/a.out")
            self.storage.cleanup()

def format(fname):
    main_params = Params()
    main_params.add_param('name', '^>>>name : (\S*)$')
    sub_params = Params()
    sub_params.add_param('num', '^>>>num : (\S*)$')
    sub_params.add_param('workload', '^>>>workload : (\S*)$')
    target_file = PraseTargetFile(fname)
    parser = Parser(target_file, '^>>>time : ([\d\.]*)ns$', main_params, sub_params)
    parser.parse()
    parser.output('num', 'name', "formatted_output")

def build_and_test(update_detector):
    env = get_env('output', **conf)
    if (not 'benchmark' in conf) or not conf["benchmark"]:
        Test(env, "test/optional.cc").build_and_run()
        Test(env, "test/status.cc").build_and_run()
        Test(env, "test/slice.cc").build_and_run()
        Test(env, "test/block_storage.cc").build_and_run()
        Test(env, "test/char_storage.cc").build_and_run()
        Test(env, "test/simple_io.cc").build_and_run()
        Test(env, "test/iterator.cc").build_and_run()
        Test(env, "test/persistence.cc").build_and_run()
        Test(env, "test/performance_evaluation.cc").build_and_run('-DNDEBUG')
    else:
        env.write_now()
        env.write(">>>name : simple_kvs\n")
        test = Test(env, "test/benchmark.cc", 20)
        for i in [1,2,4,8]:
            test.build_and_run('-DNDEBUG -DCNT={}'.format(i))
        format('output')


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
    
    update_detector.record('')

conf = {
    "ve": True,
    #"gdb": True,
    # "benchmark": True,
    #"force_test": True,
}
def main():
    update_detector = UpdateDetector(['leveldb:ve'])
    if (('force_test' in conf) and conf["force_test"]) or update_detector.is_newfile_available():
        build_and_test(update_detector)

if __name__ == "__main__":
    main()