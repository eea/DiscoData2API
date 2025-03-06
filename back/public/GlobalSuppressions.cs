// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Style", "IDE1006:Estilos de nombres", Justification = "<pendiente>", Scope = "member", Target = "~P:DiscoData2API.Model.MongoDocument._id")]
[assembly: SuppressMessage("Usage", "CA2254:La plantilla debe ser una expresión estática", Justification = "<pendiente>", Scope = "member", Target = "~M:DiscoData2API.Services.MongoService.GetFullDocumentById(System.String)~System.Threading.Tasks.Task{DiscoData2API.Model.MongoDocument}")]
[assembly: SuppressMessage("Usage", "CA2254:La plantilla debe ser una expresión estática", Justification = "<pendiente>", Scope = "member", Target = "~M:DiscoData2API.Controllers.ViewController.ExecuteQuery(System.String,DiscoData2API.Class.QueryRequest)~System.Threading.Tasks.Task{Microsoft.AspNetCore.Mvc.ActionResult{System.String}}")]
[assembly: SuppressMessage("Style", "IDE0066:Convertir una instrucción switch en expresión", Justification = "<pendiente>", Scope = "member", Target = "~M:DiscoData2API.Services.DremioService.ConvertRecordBatchToJson(Apache.Arrow.RecordBatch)~System.String")]
