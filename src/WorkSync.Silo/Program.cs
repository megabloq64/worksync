using Microsoft.Extensions.Hosting;
using WorkSync.Hosting;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { Args = args, ContentRootPath = AppContext.BaseDirectory });
builder.AddWorkSyncSilo();
await builder.Build().RunAsync();
