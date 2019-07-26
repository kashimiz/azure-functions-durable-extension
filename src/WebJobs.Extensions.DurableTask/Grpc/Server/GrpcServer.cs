using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DurableTask.Core;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Azure.WebJobs.Extensions.DurableTask.Grpc.Abstractions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Result = ClientResponse.Types.Result;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask.Grpc
{
    public class DurableRpcImpl : DurableRpc.DurableRpcBase
    {
        private DurableTaskExtension extension;

        public DurableRpcImpl(DurableTaskExtension extension)
        {
            this.extension = extension;
        }

        public override async Task<ClientResponse> ClientOperation(ClientRequest request, ServerCallContext context)
        {
            try
            {
                var attr = new OrchestrationClientAttribute()
                {
                    TaskHub = request.Config.TaskHub,
                    ConnectionName = request.Config.Connection,
                };

                var client = this.extension.GetClient(attr);
                var args = this.GetRequestArgs(request);

                var response = new ClientResponse() { Result = Result.Completed };

                // TODO: yell if args are missing/misshapen
                switch (request.Type)
                {
                    case ClientRequest.Types.Type.Create:
                        var createName = args["orchestrationFunctionName"] as string;
                        var createId = args["instanceId"] as string;
                        var createResult = await client.StartNewAsync(createName, createId, args["input"]);

                        response.Data = this.ToTypedData(createResult);
                        break;
                    case ClientRequest.Types.Type.Purge:
                        PurgeHistoryResult purgeResult;
                        if (args.TryGetValue("instanceId", out var purgeId))
                        {
                            purgeResult = await client.PurgeInstanceHistoryAsync((string)purgeId);
                        }
                        else
                        {
                            var purgeCreatedFrom = (DateTime)args["createdTimeFrom"];
                            purgeResult = await client.PurgeInstanceHistoryAsync(
                                purgeCreatedFrom,
                                args["createdTimeTo"] as DateTime?,
                                args["runtimeStatus"] as IEnumerable<OrchestrationStatus>);
                        }

                        response.Data = this.ToTypedData(purgeResult.InstancesDeleted);
                        break;
                    case ClientRequest.Types.Type.Rewind:
                        await client.RewindAsync((string)args["instanceId"], (string)args["reason"]);
                        break;
                    case ClientRequest.Types.Type.SendEvent:
                        await client.RaiseEventAsync((string)args["instanceId"], (string)args["eventName"], args["input"]);
                        break;
                    case ClientRequest.Types.Type.Status:
                        if (args.TryGetValue("instanceId", out var statusId))
                        {
                            args.TryGetValue("showHistory", out var showHistory);
                            args.TryGetValue("showHistoryOutput", out var showHistoryOutput);
                            args.TryGetValue("showInput", out var showInput);

                            var statusResult = await client.GetStatusAsync(
                                (string)statusId,
                                showHistory is bool showHistoryBool ? showHistoryBool : false,
                                showHistoryOutput is bool showHistoryOutputBool ? showHistoryOutputBool : false,
                                showInput is bool showInputBool ? showInputBool : true);

                            response.Data = this.ToTypedData(statusResult);
                        }
                        else if (args.TryGetValue("createdTimeFrom", out var statusCreatedTimeFrom))
                        {
                            args.TryGetValue("createdTimeTo", out var statusCreatedTimeTo);
                            args.TryGetValue("runtimeStatus", out var runtimeStatus);

                            var statusArgList = new List<OrchestrationRuntimeStatus>();
                            Array.ForEach<string>((string[])runtimeStatus, (str) => { statusArgList.Add(this.ToRuntimeStatus(str)); });

                            if (args.TryGetValue("pageSize", out var statusPageSize) &&
                                args.TryGetValue("continuationToken", out var statusContinuationToken))
                            {
                                var statusQueryResult = await client.GetStatusAsync(
                                    (DateTime)statusCreatedTimeFrom,
                                    statusCreatedTimeTo as DateTime?,
                                    statusArgList,
                                    (int)statusPageSize,
                                    (string)statusContinuationToken);

                                response.Data = this.ToTypedData(statusQueryResult);
                            }
                            else
                            {
                                var statusList = await client.GetStatusAsync(
                                    (DateTime)statusCreatedTimeFrom,
                                    statusCreatedTimeTo as DateTime?,
                                    statusArgList);
                                var typedData = new DurableTypedData();
                                foreach (var status in statusList)
                                {
                                    typedData.CollectionStatus.Statuses.Add(this.ToRpcOrchestrationStatus(status));
                                }

                                response.Data = typedData;
                            }
                            break;
                        }
                        else
                        {
                            var statusNoArgResult = await client.GetStatusAsync();
                        }

                        break;
                    case ClientRequest.Types.Type.Terminate:
                        var terminateId = args["instanceId"] as string;
                        var terminateReason = args["reason"] as string;
                        await client.TerminateAsync(terminateId, terminateReason);
                        break;
                    default:
                        throw new InvalidOperationException("This should never get hit; proper error handling later.");
                }

                return response;
            }
            catch (Exception ex)
            {
                return new ClientResponse()
                {
                    Result = Result.Failed,
                    Data = this.ToTypedData(ex),
                };
            }
        }

        // TODO: move into extension method
        private object ToObject(DurableTypedData data)
        {
            switch (data.DataCase)
            {
                case DurableTypedData.DataOneofCase.Bool:
                    return data.Bool;
                case DurableTypedData.DataOneofCase.Bytes:
                    return data.Bytes.ToByteArray();
                case DurableTypedData.DataOneofCase.CollectionStatus:
                    var collectionStatus = new List<DurableOrchestrationStatus>();
                    foreach (var status in data.CollectionStatus.Statuses)
                    {
                        collectionStatus.Add(this.FromRpcOrchestrationStatus(status));
                    }

                    return collectionStatus;
                case DurableTypedData.DataOneofCase.CollectionString:
                    return data.CollectionString.Strings.ToArray();
                case DurableTypedData.DataOneofCase.Int:
                    return data.Int;
                case DurableTypedData.DataOneofCase.Json:
                    return JsonConvert.DeserializeObject(data.Json);
                case DurableTypedData.DataOneofCase.None:
                    return null;
                case DurableTypedData.DataOneofCase.String:
                    return data.String;
                case DurableTypedData.DataOneofCase.Timestamp:
                    return data.Timestamp.ToDateTime();
                default:
                    throw new InvalidOperationException("Unknown RequestData type");
            }
        }

        private DurableTypedData ToTypedData(object value)
        {
            DurableTypedData typedData = new DurableTypedData();
            if (value == null)
            {
                return typedData;
            }

            if (value is byte[] arr)
            {
                typedData.Bytes = ByteString.CopyFrom(arr);
            }
            else if (value is JToken jobj)
            {
                typedData.Json = jobj.ToString();
            }
            else if (value is string str)
            {
                typedData.String = str;
            }
            else if (value is OrchestrationStatusQueryResult osqr)
            {
                typedData.CollectionStatus = new StatusCollection()
                {
                    ContinuationToken = osqr.ContinuationToken,
                };

                foreach (var status in osqr.DurableOrchestrationState)
                {
                    typedData.CollectionStatus.Statuses.Add(this.ToRpcOrchestrationStatus(status));
                }
            }
            else
            {
                typedData = this.ToRpcDefault(value);
            }

            return typedData;
        }

        private OrchestrationRuntimeStatus ToRuntimeStatus(string str)
        {
            Enum.TryParse<OrchestrationRuntimeStatus>(str, out var runtimestatus);
            return runtimestatus;
        }

        private DurableOrchestrationStatus FromRpcOrchestrationStatus(RpcOrchestrationStatus rpcStatus)
        {
            var status = new DurableOrchestrationStatus()
            {
                Name = rpcStatus.Name,
                InstanceId = rpcStatus.Id,
                CreatedTime = rpcStatus.CreatedTime.ToDateTime(),
                LastUpdatedTime = rpcStatus.LastUpdatedTime.ToDateTime(),
                Input = JToken.Parse(rpcStatus.Input),
                Output = JToken.Parse(rpcStatus.Output),
                RuntimeStatus = ToRuntimeStatus(rpcStatus.RuntimeStatus),
                CustomStatus = JToken.Parse(rpcStatus.CustomStatus),
                History = new JArray(),
            };

            foreach (var historyEvent in rpcStatus.History)
            {
                status.History.Add(JToken.Parse(historyEvent));
            }

            return status;
        }

        private RpcOrchestrationStatus ToRpcOrchestrationStatus(DurableOrchestrationStatus status)
        {
            var rpcStatus = new RpcOrchestrationStatus()
            {
                Name = status.Name,
                Id = status.InstanceId,
                CreatedTime = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(status.CreatedTime),
                LastUpdatedTime = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(status.LastUpdatedTime),
                Input = status.Input.ToString(),
                Output = status.Output.ToString(),
                RuntimeStatus = status.RuntimeStatus.ToString(),
                CustomStatus = status.CustomStatus.ToString(),
            };

            foreach (var historyEvent in status.History)
            {
                rpcStatus.History.Add(JsonConvert.SerializeObject(historyEvent));
            }

            return rpcStatus;
        }

        private DurableTypedData ToRpcDefault(object value)
        {
            DurableTypedData data = new DurableTypedData();
            try
            {
                data.Json = JsonConvert.SerializeObject(value);
            }
            catch (Exception ex)
            {
                data.String = value.ToString();
            }

            return data;
        }

        private IDictionary<string, object> GetRequestArgs(ClientRequest request)
        {
            var dict = new Dictionary<string, object>();

            foreach (var arg in request.Args)
            {
                dict.Add(arg.Key, this.ToObject(arg.Value));
            }

            return dict;
        }
    }

    public class GrpcServer : IRpcServer, IDisposable
    {
        private Server server;
        private bool disposed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="GrpcServer"/> class.
        /// </summary>
        public GrpcServer(DurableTaskExtension extension)
        {
            this.server = new Server
            {
                Services = { DurableRpc.BindService(new DurableRpcImpl(extension)) },
                Ports = { new ServerPort("127.0.0.1", ServerPort.PickUnused, ServerCredentials.Insecure) },
            };
        }

        /// <summary>
        /// TODO.
        /// </summary>
        public Uri Uri => new Uri($"http://127.0.0.1:{this.server.Ports.First().BoundPort}");

        /// <summary>
        /// TODO.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the result of the asynchronous operation.</returns>
        public Task StartAsync()
        {
            this.server.Start();
            return Task.CompletedTask;
        }

        /// <summary>
        /// TODO.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the result of the asynchronous operation.</returns>
        public Task ShutdownAsync() => this.server.ShutdownAsync();

        /// <summary>
        /// TODO.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the result of the asynchronous operation.</returns>
        public Task KillAsync() => this.server.KillAsync();

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    this.server.ShutdownAsync();
                }

                this.disposed = true;
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.Dispose(true);
        }
    }
}
