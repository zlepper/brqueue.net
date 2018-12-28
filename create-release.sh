rm -rf brqueue/bin
rm -rf brqueue/obj

dotnet build -c release
dotnet pack -c release
cd brqueue/bin/Release
dotnet nuget push brqueue.net.*.nupkg -s https://api.nuget.org/v3/index.json -k $1