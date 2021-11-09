import pickle
from icecream import ic

def save_pickle(data,filename):
    with open(filename,"wb") as fin:
        pickle.dump(data,fin)

def read_pickle(filename):
    with open(filename,"rb") as fin:
        data  = pickle.load(fin)
    return data

def update_labels(index,label_query,JSS_MAPPING):
    result_dict,level_label_mapping = build_script(label_query,JSS_MAPPING)
    flag = 0
    for label_query_key in label_query.keys():

        ic(label_query_key)
        query = label_query.get(label_query_key)
        script_json = result_dict.get(label_query_key)
        query_json = {
            "query":query["query"],
            "script": {
                "source": "if(ctx._source.industry_labels!=null) {for(int i =0 ; i<params.label_flag.length;i++  ) {if(!ctx._source.industry_labels.contains(params.label_flag[i])){ctx._source.industry_labels.add(params.label_flag[i])}} } else{ctx._source.industry_labels = params.label_flag}",
                "params": {
                    "label_flag": script_json
                },
                "lang": "painless"
            }
        }
        print(query_json["script"])
        prefix_task_id = es.update_by_query(index=index, body=query_json,refresh = True,wait_for_completion= False)
        stop = True
        while stop:
             status = es.tasks.get(prefix_task_id["task"])
             if status["completed"]:
                  stop = False
                  updated_prefix_num = status["task"]["status"]["updated"]
                  ic("更新完成 ！！！")
                  ic(updated_prefix_num)
