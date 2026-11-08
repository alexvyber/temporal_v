package main

import (
    "encoding/json"
    "os"
    "fmt"
    "sort"
    "go.opentelemetry.io/otel/baggage"
)

type Property struct {Key string `json:"key"`;Value string `json:"value"`;HasValue bool `json:"has_value"`}
type Member struct {Key string `json:"key"`;Value string `json:"value"`;Properties []Property `json:"properties"`}
type Result struct {Members []Member `json:"members"`;Header string `json:"header"`;Error bool `json:"error"`}
func main() {
    data,err:=os.ReadFile(os.Args[1]);if err!=nil {panic(err)}
    var cases []string;if err=json.Unmarshal(data,&cases);err!=nil {panic(err)}
    for _,input:=range cases {
        bag,parseError:=baggage.Parse(input)
        members:=bag.Members();sort.Slice(members,func(i,j int) bool{return members[i].Key()<members[j].Key()})
        result:=Result{Members:[]Member{},Error:parseError!=nil}
        headers:=[]string{}
        for _,member:=range members {
            value:=Member{Key:member.Key(),Value:member.Value(),Properties:[]Property{}}
            for _,property:=range member.Properties(){text,present:=property.Value();value.Properties=append(value.Properties,Property{property.Key(),text,present})}
            result.Members=append(result.Members,value);headers=append(headers,member.String())
        }
        for index,header:=range headers {if index>0 {result.Header+=","};result.Header+=header}
        encoded,err:=json.Marshal(result);if err!=nil{panic(err)};fmt.Println(string(encoded))
    }
}
