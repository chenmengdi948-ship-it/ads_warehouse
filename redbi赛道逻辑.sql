赛道行业：if [track_detail_name]="其他" and [dept4]="美妆洗护行业" then "美妆" 
else if [track_detail_name]="其他" and [dept4]="奢品行业" then "奢品" 
else if [track_detail_name]="其他" and [dept4]="服饰潮流行业" then "服饰潮流" 
else if [track_detail_name]="其他" then "暂无赛道行业" else [track_industry_name]
一级赛道：if [track_detail_name]="其他" then "暂无一级赛道" else [track_group_name]
二级赛道：if ISNULL(rawsql("String", "split_part(%1, '-', 3)", [track_detail_name]) ) then "暂无二级赛道" ELSE rawsql("String", "split_part(%1, '-', 3)", [track_detail_name])
三级赛道：if ISNULL(rawsql("String", "split_part(%1, '-', 4)", [track_detail_name]) ) then "暂无三级赛道" ELSE rawsql("String", "split_part(%1, '-', 4)", [track_detail_name])

赛道行业划分：if [赛道行业（处理后）] in ("美妆","奢品","服饰潮流") then "美奢潮流服饰" 
else if [赛道行业（处理后）] in ( "3C家电","交通出行","互联网","家居家装","房地产") then "耐消" 
else if [赛道行业（处理后）] in ( "食品饮料","母婴","大健康","宠物") then "快消" else [赛道行业（处理后）]