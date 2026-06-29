using ServerWorker;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<PipeWorker>();

var host = builder.Build();
await host.RunAsync();