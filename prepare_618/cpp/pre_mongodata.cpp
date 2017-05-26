#include <cstdint>
#include <iostream>
#include <vector>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;


class PreMongoData
{
public:
	// read data from MonogDB
	int ReadData(){
		// connect to MongoDB
		mongocxx::client client{mongocxx::uri{}};
		// connect to database
		mongocxx::database db = client["cpptest"];
		// connect to collection
		mongocxx::collection coll = db["tmall618"];

        /*
		mongocxx::cursor cursor = coll.find(document{} << "catID" << "1101050203" << finalize);
        for(auto doc : cursor) {
            std::cout << bsoncxx::to_json(doc) << "\n";
        };
        */
		return 0;
	};
    
    // process data 
	int ProcessData(int data){
		return 0;
	};
	
	// update data into MongoDB
	int UpdateData(int data){
		return 0;
	};
};


int PreData()
{    
	int data;
	int indicator;
	PreMongoData pre;
	data = pre.ReadData();
	data = pre.ProcessData(data);
	indicator = pre.UpdateData(data);
	return indicator;
};


int main()
{
	int i;
	i = PreData();
	return i;
};