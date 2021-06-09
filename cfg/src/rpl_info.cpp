#include "rpl_info.h"

Rpl_info::Rpl_info() {
    tinyxml2::XMLDocument doc;
    doc.LoadFile("../rpl_cfg.xml");
    if (doc.Error()){
        std::cout<<"Read XML Fail!"<<std::endl;
    }
    tinyxml2::XMLElement *info = doc.RootElement();  // info为根节点
    is_remote = info->FirstChildElement("is_remote")->BoolText();
    is_single_group = info->FirstChildElement("is_single_group")->BoolText();
    parse_pool = info->FirstChildElement("parse_pool")->IntText();
    interval = info->FirstChildElement("interval")->IntText();
    auto node = info->FirstChildElement( "files");
    if (node){
        tinyxml2::XMLElement* list = node->FirstChildElement( "file");
        while (list){
            files.push_back(list->GetText());
            list = list->NextSiblingElement("file");
        }
    }
    node = info->FirstChildElement("group");
    uint32_t gid = 0;
    if (node){
        group_num = node->FirstChildElement("group_num")->IntText();
        tinyxml2::XMLElement* list = node->FirstChildElement( "group_cfg");
        while (list){
//            uint32_t gid = list->FirstChildElement("group_id")->IntText();
            uint32_t pool = list->FirstChildElement("pool")->IntText();
            group_pool.insert(group_pool.begin()+gid, pool);
            tinyxml2::XMLElement* table_list = list->FirstChildElement( "group_table");
            while (table_list){
                std::string table = table_list->GetText();
                group_map[table] = gid;
//                table = table.substr(table.find(".")+1);
//                all_tables.push_back(table);
                table_list = table_list->NextSiblingElement("group_table");
            }
            list = list->NextSiblingElement("group_cfg");
            gid++;
        }
    }
    tinyxml2::XMLElement* list = info->FirstChildElement( "tables");
    tinyxml2::XMLElement* table_list = list->FirstChildElement( "table");
    while (table_list){
        std::string table = table_list->GetText();
        all_tables.push_back(table);
        table_list = table_list->NextSiblingElement("table");
    }

    node = info->FirstChildElement("query_freqs");
    if (node){
        tinyxml2::XMLElement* list = node->FirstChildElement( "query_ferq");
        while (list){
            query_freq q;
            q.ferq = list->FirstChildElement("freq")->IntText();
            tinyxml2::XMLElement* table_list = list->FirstChildElement( "query_tables");
            while (table_list){
                std::string table = table_list->GetText();
                int g = group_map[table];
                q.query_group.insert(g);
                table_list = table_list->NextSiblingElement("query_tables");
            }
            query_fre.push_back(q);
            list = list->NextSiblingElement("query_ferq");
        }
    }
}