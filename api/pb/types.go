package pb

import innerpb "kvraft/api/pb/kvraft/api/pb"

// 兼容层：将 kvraft/api/pb 下的类型对齐到真实的 gRPC 生成类型，
// 避免脚本/工具侧因历史手写类型与 proto 漂移而出现请求字段不匹配。

type GetRequest = innerpb.GetRequest
type GetResponse = innerpb.GetResponse

type PutRequest = innerpb.PutRequest
type PutResponse = innerpb.PutResponse

type DeleteRequest = innerpb.DeleteRequest
type DeleteResponse = innerpb.DeleteResponse

type ScanRequest = innerpb.ScanRequest
type ScanResponse = innerpb.ScanResponse

type KeyValue = innerpb.KeyValue

type WatchCreateRequest = innerpb.WatchCreateRequest
type WatchCancelRequest = innerpb.WatchCancelRequest
type WatchRequest = innerpb.WatchRequest
type WatchEvent = innerpb.WatchEvent

type ClusterStatusRequest = innerpb.ClusterStatusRequest
type ClusterStatusResponse = innerpb.ClusterStatusResponse
type NodeStatus = innerpb.NodeStatus

type KVServiceClient = innerpb.KVServiceClient
type KVServiceServer = innerpb.KVServiceServer

var NewKVServiceClient = innerpb.NewKVServiceClient
