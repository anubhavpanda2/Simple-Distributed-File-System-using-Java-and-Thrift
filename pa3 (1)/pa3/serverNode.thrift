service serverNode {
bool ping(),
string write(string fileName,string contents),
string writeAll(string filename,string contents),
string read(string filename),
string readAll(string filename),
i32 getversion(string filename),
void updateVersion(string filename,i32 version),
map<string, i32> getMap()
}
