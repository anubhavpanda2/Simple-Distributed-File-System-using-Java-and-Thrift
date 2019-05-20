service coordinatorNode {
bool ping(),
void join(string ip,string port),
string assembleQuorom(string ip,string port,i32 task,string FileName),
void finished(string ip,string port,i32 task,i32 id,string FileName),
void synch()
}
