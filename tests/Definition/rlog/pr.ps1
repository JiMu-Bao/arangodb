Function global:registerTests()
{
    noteStartAndRepoState

    Write-Host "Registering tests..."

    $global:TESTSUITE_TIMEOUT = 3900

    registerTest -testname "gtest_replication2"
    registerTest -testname "replication2_server" -cluster $true
    registerTest -testname "auto" -cluster $true -filter "tests\\js\\common\\shell\\shell-replicated-logs-cluster.js"
    registerTest -testname "auto" -cluster $true -filter "tests\\js\\common\\shell\\shell-prototype-state-cluster.js"
    comm
}
