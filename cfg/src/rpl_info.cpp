#include "rpl_info.h"

Rpl_info::Rpl_info() {
    tinyxml2::XMLDocument doc;
    doc.LoadFile("/root/project/mts/rpl_cfg.xml");
    if (doc.Error()){
        std::cout<<"Read XML Fail!"<<std::endl;
    }
    tinyxml2::XMLElement *info = doc.RootElement();  // info为根节点
    is_remote = info->FirstChildElement("is_remote")->BoolText();
    is_single_group = info->FirstChildElement("is_single_group")->BoolText();
    parse_pool = info->FirstChildElement("parse_pool")->IntText();
    group_num = info->FirstChildElement("group_num")->IntText();
    auto node = info->FirstChildElement( "files");
    if (node){
        tinyxml2::XMLElement* list = node->FirstChildElement( "file");
        while (list){
            files.push_back(list->GetText());
            list = list->NextSiblingElement("file");
        }
    }
    node = info->FirstChildElement( "group_pool");
    if (node){
        tinyxml2::XMLElement* list = node->FirstChildElement( "pool");
        while (list){
            group_pool.push_back(list->IntText());
            list = list->NextSiblingElement("pool");
        }
    }
}