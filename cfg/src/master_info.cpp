#include "master_info.h"

Master_info::Master_info() {
    tinyxml2::XMLDocument doc;
    doc.LoadFile("../master_cfg.xml");
    if (doc.Error()){
        std::cout<<"Read XML Fail!"<<std::endl;
    }
    tinyxml2::XMLElement *info = doc.RootElement();  // info为根节点
    user_name = info->FirstChildElement("user_name")->GetText();  //以名字获取根节点下的子节点
    pwd = info->FirstChildElement("password")->GetText();
    host = info->FirstChildElement("host")->GetText();
    port = info->FirstChildElement("port")->IntText();
//    std::cout<<user_name<<" "<<pwd<<" "<<ip<<" "<<port<<std::endl;
}