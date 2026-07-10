import os

# 1. Patch build.xml
content = open('build.xml').read()

old_str = """    <!-- Non-java resources needed by the test suite -->
    <copy todir="${test.classes}">
      <fileset dir="${test.resources}"/>
    </copy>
  </target>"""

new_str = old_str + """

  <target name="microbench-build" depends="_main-jar,resolver-dist-lib" description="Compile only what is needed for microbench">
    <mkdir dir="${test.classes}"/>
    <javac
     compiler="modern"
     debug="true"
     debuglevel="${debuglevel}"
     destdir="${test.classes}"
     includeantruntime="true"
     source="${ant.java.version}"
     target="${ant.java.version}"
     encoding="utf-8">
     <classpath>
        <path refid="cassandra.classpath.test"/>
     </classpath>
     <compilerarg value="-XDignore.symbol.file"/>
     <compilerarg line="${jdk11plus-javac-exports}"/>
     <src path="${test.anttasks.src}"/>
     <src path="${test.unit.src}"/>
     <src path="${test.microbench.src}"/>
     <src path="${test.distributed.src}"/>
     <src path="${test.harry.src}"/>
     <exclude name="**/*Test.java"/>
     <exclude name="**/*TestBase.java"/>
     <exclude name="**/distributed/test/**/*.java"/>
     <exclude name="**/CassandraBriefJUnitResultFormatter.java"/>
     <exclude name="**/CassandraXMLJUnitResultFormatter.java"/>
     <exclude name="**/JStackJUnitTask.java"/>
    </javac>
    <javac
     compiler="modern"
     debug="true"
     debuglevel="${debuglevel}"
     destdir="${test.classes}"
     includeantruntime="true"
     source="${ant.java.version}"
     target="${ant.java.version}"
     encoding="utf-8">
     <classpath>
        <path refid="cassandra.classpath.test"/>
        <pathelement location="${test.classes}"/>
     </classpath>
     <compilerarg value="-XDignore.symbol.file"/>
     <compilerarg line="${jdk11plus-javac-exports}"/>
     <src path="${test.classes}"/>
     <include name="**/jmh_generated/*.java"/>
    </javac>
    <!-- Non-java resources needed by the test suite -->
    <copy todir="${test.classes}">
      <fileset dir="${test.resources}"/>
    </copy>
  </target>"""

if "microbench-build" not in content and old_str in content:
    open('build.xml', 'w').write(content.replace(old_str, new_str, 1))
    print("build.xml successfully patched")
else:
    print("build.xml already patched or marker not found")

# 2. Patch TlsTestUtils.java
tls_file = 'test/unit/org/apache/cassandra/transport/TlsTestUtils.java'
if os.path.exists(tls_file):
    tls_content = open(tls_file).read()
    
    old_imports = """import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.util.Auth;
import org.apache.cassandra.distributed.util.SingleHostLoadBalancingPolicy;"""

    new_imports = """// import org.apache.cassandra.distributed.api.ICluster;
// import org.apache.cassandra.distributed.api.IInvokableInstance;
// import org.apache.cassandra.distributed.shared.ClusterUtils;
// import org.apache.cassandra.distributed.util.Auth;
// import org.apache.cassandra.distributed.util.SingleHostLoadBalancingPolicy;"""

    old_method1 = """    public static void configureIdentity(ICluster<IInvokableInstance> cluster, SSLOptions sslOptions)
    {
        withAuthenticatedSession(cluster.get(1), DEFAULT_SUPERUSER_NAME, DEFAULT_SUPERUSER_PASSWORD, session -> {
            session.execute("CREATE ROLE cassandra_ssl_test WITH LOGIN = true");
            session.execute(String.format("ADD IDENTITY '%s' TO ROLE 'cassandra_ssl_test'", CLIENT_SPIFFE_IDENTITY));
            // GRANT select to cassandra_ssl_test to be able to query the system_views.clients virtual table
            session.execute("GRANT SELECT ON system_views.clients to cassandra_ssl_test");
        }, sslOptions);
    }"""

    new_method1 = """    /*
    public static void configureIdentity(ICluster<IInvokableInstance> cluster, SSLOptions sslOptions)
    {
        withAuthenticatedSession(cluster.get(1), DEFAULT_SUPERUSER_NAME, DEFAULT_SUPERUSER_PASSWORD, session -> {
            session.execute("CREATE ROLE cassandra_ssl_test WITH LOGIN = true");
            session.execute(String.format("ADD IDENTITY '%s' TO ROLE 'cassandra_ssl_test'", CLIENT_SPIFFE_IDENTITY));
            // GRANT select to cassandra_ssl_test to be able to query the system_views.clients virtual table
            session.execute("GRANT SELECT ON system_views.clients to cassandra_ssl_test");
        }, sslOptions);
    }
    */"""

    old_method2 = """    public static void withAuthenticatedSession(IInvokableInstance instance,
                                         String username,
                                         String password,
                                         Consumer<Session> consumer,
                                         SSLOptions sslOptions)
    {
        // wait for existing roles
        Auth.waitForExistingRoles(instance);

        InetSocketAddress nativeInetSocketAddress = ClusterUtils.getNativeInetSocketAddress(instance);
        InetAddress address = nativeInetSocketAddress.getAddress();
        LoadBalancingPolicy lbc = new SingleHostLoadBalancingPolicy(address);

        com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder()
                                                                                           .withLoadBalancingPolicy(lbc)
                                                                                           .withSSL(sslOptions)
                                                                                           .withAuthProvider(new PlainTextAuthProvider(username, password))
                                                                                           .addContactPoint(address.getHostAddress())
                                                                                           .withPort(nativeInetSocketAddress.getPort());

        try (com.datastax.driver.core.Cluster c = builder.build(); Session session = c.connect())
        {
            consumer.accept(session);
        }
    }"""

    new_method2 = """    /*
    public static void withAuthenticatedSession(IInvokableInstance instance,
                                         String username,
                                         String password,
                                         Consumer<Session> consumer,
                                         SSLOptions sslOptions)
    {
        // wait for existing roles
        Auth.waitForExistingRoles(instance);

        InetSocketAddress nativeInetSocketAddress = ClusterUtils.getNativeInetSocketAddress(instance);
        InetAddress address = nativeInetSocketAddress.getAddress();
        LoadBalancingPolicy lbc = new SingleHostLoadBalancingPolicy(address);

        com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder()
                                                                                           .withLoadBalancingPolicy(lbc)
                                                                                           .withSSL(sslOptions)
                                                                                           .withAuthProvider(new PlainTextAuthProvider(username, password))
                                                                                           .addContactPoint(address.getHostAddress())
                                                                                           .withPort(nativeInetSocketAddress.getPort());

        try (com.datastax.driver.core.Cluster c = builder.build(); Session session = c.connect())
        {
            consumer.accept(session);
        }
    }
    */"""

    if old_imports in tls_content:
        tls_content = tls_content.replace(old_imports, new_imports, 1)
        tls_content = tls_content.replace(old_method1, new_method1, 1)
        tls_content = tls_content.replace(old_method2, new_method2, 1)
        open(tls_file, 'w').write(tls_content)
        print("TlsTestUtils.java successfully patched")
    else:
        print("TlsTestUtils.java already patched or markers not found")
