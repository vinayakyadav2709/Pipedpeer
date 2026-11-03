To the Editor-in-Chief,
Journal of Parallel and Distributed Computing

Dear Editor,

We submit the manuscript “PipedPeer: Running Unmodified Python Across a Small Group's Machines Without a Configured Cluster” for consideration as a research paper in the Journal of Parallel and Distributed Computing.

Cluster frameworks such as Spark, Ray and Dask assume a configured cluster, a program written against their interface, and machines that can reach one another. The manuscript presents PipedPeer, a system that removes all three assumptions for Python. It makes the following contributions:

- It runs an unmodified Python script on peer machines, with one identical daemon per machine and no head node.
- It ships the script's environment as a Nix closure pinned to one package-set revision, so a peer receives only the store paths it lacks.
- At import time it substitutes parallel maps, joblib, pandas grouping and joins, NumPy products and PyTorch data-parallel training. A map is divided by a makespan model fitted to measured peer rates, and numerical work moves only when a cost model predicts a gain.
- Machines on different networks meet through an introducer that every daemon serves, then connect over direct, authenticated QUIC links. No job data is relayed.
- It is evaluated in container labs and on physical machines in different homes, including a phone. The evaluation reports the cases where distribution was slower as well as those where it was faster.

The work fits the journal's scope on the design, implementation and evaluation of distributed systems, in particular peer-to-peer computing, scheduling on heterogeneous machines and data-parallel training.

The manuscript is original. It has not been published and is not under consideration for publication elsewhere. All authors have approved the manuscript and agree with its submission. The authors declare no competing interests. The source code and every measurement record cited are publicly available in the project repository.

Sincerely,

Yateen Vaviya (corresponding author), on behalf of all authors
Department of Computer Engineering, Sardar Patel Institute of Technology, Mumbai, India
yateen.vaviya23@spit.ac.in
