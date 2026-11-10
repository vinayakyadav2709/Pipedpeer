Emulated, containers on one PC, cgroup v2 (this PC, 15 CPUs pinned 1-15). Lab: pc00 4096m (Ray head), pc01/pc02 2000m (not 2560m: lab.sh cap host RAM-5 GB refused 9.5 GB), driver 2048m.
Ray (e19 --n 1 --add 1 --repeats 1 --systems ray): run 131 s total; baseline 15.3 s, join 18.2 s, result_ok 1, join_status ok.
Head ubu1 during run: peak memory.current 3.76 GB (anon peak 2.93 GB), memory.events max +908 (13566 -> 14474), high 0, oom 0, pressure some avg10 peak 1.29, host load1 peak 7.3.
E17 (--n 2 --joiners 1 --repeats 2): see e17.csv. Prep needed calibrate.sh --target 5 and run.sh REF --sizes 1 (702 s) first.
