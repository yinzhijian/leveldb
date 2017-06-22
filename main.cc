#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "db/write_batch_internal.h"
#include "db/memtable.h"
#include "db/log_reader.h"
#include "db/version_edit.h"
#include "db/dbformat.h"
#include "util/coding.h"
#include <cassert>
#include <iostream>

using namespace std;
using namespace leveldb;
class Test{
    public:
    explicit Test(){};
    //explicit Test(const std::string* s):s_(s){}
    explicit Test(const Test* test){
        cout<<"Test construct"<<endl;
    }
};
class Wrapper{
    private:
    const Test test_;
    public:
    //explicit Test(const std::string* s):s_(s){}
    explicit Wrapper():test_(new Test()){};
};
int write() {
    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "testdb", &db);
    assert(status.ok());
    leveldb::WriteBatch *wb = new leveldb::WriteBatch();
    wb->Put("JimZuoLin", "Hello Jim!");
    wb->Put("Yintao", "Hello Tao!");
    wb->Put("Yanxin", "Hello Xin!");
    wb->Delete("Yin");
    wb->Delete("Yan");
    wb->Delete("Yintao");

    //string res;
    status = db->Write(WriteOptions(), wb);
    assert(status.ok());
    status = db->Put(WriteOptions(), "Yanxin","last");
    assert(status.ok());
    //cout << res << endl;

    delete db;
    return 0;
}
int readManifest(std::string fileName){
    leveldb::Env* env = leveldb::Env::Default();
    SequentialFile *sf;
    leveldb::Status status;
    status = env->NewSequentialFile(fileName,&sf);
    if(status.ok()){
        leveldb::log::Reader *reader = new leveldb::log::Reader(sf,NULL,true,0);
        leveldb::Slice *slice = new leveldb::Slice(); 
        std::string* buffer = new std::string();
        while (reader->ReadRecord(slice,buffer)){
            leveldb::VersionEdit *ve = new leveldb::VersionEdit();
            ve->DecodeFrom(*slice);
            cout << ve->DebugString()<<endl;
        }
    }

}
int readLog(std::string fileName){
    //write();
    leveldb::Env* env = leveldb::Env::Default(); SequentialFile *sf;
    //std::string fileName = "/Users/yintao/Workspaces/blockchain/ycoin/ycoindb/000378.log";
    leveldb::Status status;
    status = env->NewSequentialFile(fileName,&sf);
    std::cout << status.ToString() <<endl;
    if(status.ok()){
        leveldb::log::Reader *reader = new leveldb::log::Reader(sf,NULL,true,0);
        leveldb::Slice *slice = new leveldb::Slice(); 
        std::string* buffer = new std::string();
        uint64_t seq_number;
        InternalKeyComparator comp(leveldb::BytewiseComparator());
        leveldb::MemTable *mem = new leveldb::MemTable(comp);
        while (reader->ReadRecord(slice,buffer)){
            Slice s(slice->data(),slice->size());
            leveldb::WriteBatch *wb = new leveldb::WriteBatch();
            leveldb::WriteBatchInternal::SetContents(wb,s);
            leveldb::WriteBatchInternal::InsertInto(wb,mem);
            std::cout<<"buffer size:"<<buffer->size()<<endl;
        //leveldb::GetVarint32(slice,&key_length);
        //std::cout << key_length<< endl;
            seq_number = leveldb::DecodeFixed64(slice->data());
            slice->remove_prefix(8);
            std::cout <<"seq:"<< seq_number<< endl;
            uint32_t count =leveldb::DecodeFixed32(slice->data());
            std::cout <<"count:"<< count << endl;
            slice->remove_prefix(4);
            Slice key,value;
            while(!slice->empty()){
                char tag = (*slice)[0];
                printf("tag:%d\n",tag);
                slice->remove_prefix(1);
                if(tag == leveldb::kTypeDeletion){
                    GetLengthPrefixedSlice(slice, &key);
                    printf("delete key:%s\n",key.ToString().c_str());
                } else if(tag == leveldb::kTypeValue){
                    GetLengthPrefixedSlice(slice, &key);
                    GetLengthPrefixedSlice(slice, &value);
                    printf("key:%s,value:%s\n",key.ToString().c_str(),value.ToString().c_str());
                }
            }
        }
        leveldb::Iterator *iter = mem->NewIterator();
        for(iter->SeekToFirst();iter->Valid();iter->Next()){
            leveldb::ParsedInternalKey parsedKey;
            leveldb::ParseInternalKey(iter->key(), &parsedKey);
            printf("key:%s,value:%s\n",parsedKey.DebugString().c_str(),iter->value().ToString().c_str());
        }
        Slice k("Yintao");
        std::string value;
        leveldb::Status status;
        uint64_t num = 13;
        leveldb::LookupKey key(k,num);
        mem->Get(key,&value,&status);
        printf("self get,value:%s\n",value.c_str());
        printf("mem table size:%lu\n",mem->ApproximateMemoryUsage());

        //leveldb::Slice *keySlice = new leveldb::Slice(slice->data(),key_length);
        //leveldb::ParsedInternalKey parsed;
        //leveldb::ParseInternalKey(*slice,&parsed);
        //std::cout << parsed.user_key.ToString()<<endl;
        //std::cout << buffer<<endl;
    }
    return 0;
}
int main(){
    //write();
    Wrapper wapper;
    std::string fileName = "/Users/yintao/Workspaces/third-part/leveldb/testdb/000009.log";
    readLog(fileName);
    ////fileName = "/Users/yintao/Workspaces/third-part/leveldb/testdb/MANIFEST-000004";
    //fileName = "/Users/yintao/Workspaces/blockchain/ycoin/ycoindb/MANIFEST-000034";
    //readManifest(fileName);
    return 0;
}
