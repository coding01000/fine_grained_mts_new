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
    auto node = info->FirstChildElement( "files");
    if (node){
        tinyxml2::XMLElement* list = node->FirstChildElement( "file");
        while (list){
            files.push_back(list->GetText());
            list = list->NextSiblingElement("file");
        }
    }
    node = info->FirstChildElement("group");
    if (node){
        group_num = node->FirstChildElement("group_num")->IntText();
        tinyxml2::XMLElement* list = node->FirstChildElement( "group_cfg");
        while (list){
            uint32_t gid = list->FirstChildElement("group_id")->IntText();
            uint32_t pool = list->FirstChildElement("pool")->IntText();
            group_pool.insert(group_pool.begin()+gid, pool);
            tinyxml2::XMLElement* table_list = list->FirstChildElement( "group_table");
            while (table_list){
                std::string table = table_list->GetText();
                group_map[table] = gid;
                table_list = table_list->NextSiblingElement("group_table");
            }
            list = list->NextSiblingElement("group_cfg");
        }
    }
}