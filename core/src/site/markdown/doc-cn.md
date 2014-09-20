#	文档

##	简介

HA-JDBC 是一个JDBC代理,它允许Java应用通过JDBC接口透明的访问集群的数据库.

![Database cluster access via HA-JDBC](images/ha-jdbc.png)

HA-JDBC 相较于普通 JDBC 有以下优势:

高有效性
:	数据库集群会一直服务,直到最后一个数据库节点也失效.

容错性
:	因为HA-JDBC是通过JDBC的接口进行操作的,因此它有事务的意识,可以从数据库节点失败中恢复并且不会使当前的事务失败或破坏.

可扩展性
:	通过在数据库之间平衡读请求,HA-JDBC可以水平提高负载规模(例如: 添加数据节点).


##	运行要求

*	Java 1.6+
*	对于底层的数据库,需要 JDBC 4.x 驱动
*	可选的依赖JAR包可在 [依赖](dependencies.html) 界面下载.
*	可通过XML来配置数据库集群或通过编程的方式来配置.

##	配置

HA-JDBC 通常是通过XML文件进行配置.
所有HA-JDBC XML配置模式定义列举在[XML 模式](schemas.html) 页面.


###	<a name="xml"/>XML

在运行时定位配置文件位置的算法如下:

1.	尝试从以下资源定位参数化的资源名:
	1.	传递给 `DriverManager.getConnection(String, Properties)` 的 `config` 属性, 或 `DataSource`, `ConnectionPoolDataSource`,  `XADataSource` 的 `config` 属性.
	1.	使用系统属性 ha-jdbc.*cluster-id*.configuration .
	1.	使用默认值: `ha-jdbc-{0}.xml`
1. 使用集群的标识符格式化参数化的资源名.
1. 转换格式化后的资源名为URL.如果资源不是一个URL,将使用以下类加载器在类路径中搜索资源.
	1. 线程上下文类加载器
	1. 加载HA-JDBC的类加载器
	1. 系统类加载器


###	<a name="database"/>定义数据库

通常定义HA-JDBC的集群如下:

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster>
			<database id="..." location="..." weight="#">
				<user>...</user>
				<password>...</password>
				<property name="...">...</property>
				<!-- 其它属性 -->
			</database>
		</cluster>
	</ha-jdbc>

id
:	集群中该数据库的唯一标识符

location
:	该值通常用于表示数据库的位置.
	对于基于驱动(Driver)的集群,该值用于指定数据库JDBC的url.
	对于基于数据源(DataSource)的集群,该值用于指定数据源的实现类(即会被直接实例化的类).

weight
:	定义该数据节点的相对权重.
	如果没有定义,则默认值为 1.
	具体请参见 [负载均衡](#balancer) 小节.

user
:	HA-JDBC连接数据库的用户名,主要用于同步和元数据缓存.
	该用户应该是管理员权限级别的.这不同于应用常用的数据库用户(通常用于进行增删改查或只有只读权限).

password
:	上述数据库用户的密码

property
:	定义属性集合,具体的语义依赖于数据库访问模式.
	对于基于驱动(Driver)的集群,该属性会传递给相应的`Driver.connect(String, Properties)`方法.
	对于基于数据源(DataSource)的集群,该属性会用于初始化数据源实例的JavaBean属性.
	例如:
	
		<database id="db1" location="org.postgresql.ds.PGSimpleDataSource">
			<property name="serverName">server1</property>
			<property name="portNumber">5432</property>
			<property name="databaseName">database</property>
		</database>

###	<a name="dialect"/>方言

集群的方言(dialect)属性用于对指定的数据库提供商选择合适的HA-JDBC适配器.
HA-JDBC 包含了以下数据库的方言:
<table>
	<tr>
		<th>Dialect</th>
		<th>Database vendor</th>
		<th>Sequences</th>
		<th>Identity columns</th>
		<th>Dump/restore</th>
	</tr>
	<tr>
		<td>**db2**</td>
		<td>IBM DB2</td>
		<td>&#x2714;</td>
		<td>&#x2714;</td>
		<td></td>
	</tr>
	<tr>
		<td>**derby**</td>
		<td>Apache Derby</td>
		<td>&#x2714;</td>
		<td>&#x2714;</td>
		<td></td>
	</tr>
	<tr>
		<td>**firebird**</td>
		<td>Firebird, InterBase</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**h2**</td>
		<td>H2 Database</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**hsqldb**</td>
		<td>HSQLDB</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**ingres**</td>
		<td>Ingres</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**maxdb**</td>
		<td>MySQL MaxDB, SAP DB</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**mckoi**</td>
		<td>Mckoi SQL Database</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**mysql**</td>
		<td>MySQL</td>
		<td></td>
		<td></td>
		<td>&#x2714;</td>
	</tr>
	<tr>
		<td>**oracle**</td>
		<td>Oracle Database</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**postgresql**</td>
		<td>PostgreSQL</td>
		<td>&#x2714;</td>
		<td>&#x2714;</td>
		<td>&#x2714;</td>
	</tr>
	<tr>
		<td>**sybase**</td>
		<td>Sybase</td>
		<td></td>
		<td>&#x2714;</td>
		<td></td>
	</tr>
	<tr>
		<td>***standard***</td>
		<td>SQL-92 compliant database</td>
		<td></td>
		<td></td>
		<td></td>
	</tr>
</table>

例如:

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster dialect="postgresql">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	<a name="balancer"/>负载均衡

当在集群中执行一个读请求时,HA-JDBC会使用配置的均衡策略来决定该请求会由那个数据库来处理.
每个数据库都可以定义权重来影响该均衡策略的优先级.
如果没有指定权重,则默认为1.

注: 通常情况下, 如果一个节点的权重为0,则该节点不会接受任何请求,除非该节点是集群中的最后一个节点.

默认情况下, HA-JDBC支持以下四种均衡策略:

simple
:	请求总是有最高权重的节点来处理.

random
:	随机选择节点来处理请求.
	节点的权重会影响节点被选中的几率.
	节点被选中的几率= *权重* / *总权重*.
	

round-robin
:	请求依次发往每个节点.如果一个节点的权重为*n*,在进行下一次循环之前会收到*n*次请求.

*load*
:	请求发往负载最小的节点.
	节点的权重会影响该节点的负载值.
	节点负载 = *并发请求* / *权重*.

例如:

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster balancer="simple">
			<!-- 读请求总会更倾向于选择db1 -->
			<database id="db1" location="..." weight="2"><!-- ... --></database>
			<database id="db2" location="..." weight="1"><!-- ... --></database>
		</cluster>
	</ha-jdbc>


###	<a name="sync"/>同步策略

定义数据库激活前的同步策略.
一个集群可指定多个同步策略,当然,其中一个必须指定为 `default-sync`.
当自动激活时触发的同步策略为默认的同步策略.
其他的策略只用于手动激活数据库.


HA-JDBC 默认支持以下策略.
在3.0版本时,同步策略是通过标识符定义的而不是类名.
如果策略暴露了任何JavaBean属性,那么这些属性可通过内嵌的`property`元素来从写.

passive
:	什么都不做.
	对于只读集群可使用该策略,或者你已经知道目标数据库已经同步了.

dump-restore
:	执行原数据库到目标数据库的本地转储/恢复.
	要求使用的方言支持才能使用该策略(参考 [Dialect.getDumpRestoreSupport()](apidocs/net/sf/hajdbc/dialect/Dialect.html))
	和其他策略不同,该策略可同时同步模式和数据.

full
:	清空目标数据库中的所有表然后从源数据库插入值.
	<table>
		<tr>
			<th>属性</th>
			<th>默认</th>
			<th>描述</th>
		</tr>
		<tr>
			<td>**fetchSize**</td>
			<td>0</td>
			<td>控制从源数据库一次获取的行数.</td>
		</tr>
		<tr>
			<td>**maxBatchSize**</td>
			<td>100</td>
			<td>控制一次批执行插入/更新/删除的最大个数</td>
		</tr>
	</table>
	
diff
:	执行源表和目标表的全表扫描对比,执行必要的insert/update/delete.
	支持一下属性:
	<table>
		<tr>
			<th>属性</th>
			<th>默认</th>
			<th>描述</th>
		</tr>
		<tr>
			<td>**versionPattern**</td>
			<td></td>
			<td>
				指定一个匹配列名的正则表达式,该列内容为上次更新时间戳.
				如果指定了, 可通过列版本对比来判断行是否需要更新,而不是执行全列扫描.
			</td>
		</tr>
		<tr>
			<td>**fetchSize**</td>
			<td>0</td>
			<td>控制从源数据库一次获取的行数.</td>
		</tr>
		<tr>
			<td>**maxBatchSize**</td>
			<td>100</td>
			<td>控制一次批执行插入/更新/删除的最大个数</td>
		</tr>
	</table>

例如:

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<sync id="full">
			<property name="fetchSize">1000</property>
		</sync>
		<sync id="diff">
			<property name="fetchSize">1000</property>
			<property name="versionPattern">version</property>
		</sync>
		<cluster default-sync="diff"><!-- ... --></cluster>
	</ha-jdbc>


###	<a name="state"/>集群状态管理

状态管理模块主要负责存储集群中数据库的激活状态,和很多的持久状态类似.
在启动时,HA-JDBC获取它的初始集群状态.如果HA-JDBC配置为分布的(distributable)或状态管理为持久的,则会向其它服务获取该状态.
如果没有找到状态,所有对数据库的访问都认为是激活的.
如果想要在启动时忽略本地持久的集群状态,可使用 *ha-jdbc.state.clear=true* 系统属性.

HA-JDBC 包含了一下状态管理器实现:

simple
:	非持久的状态管理器,在内存中记录集群状态.
	例如:

		<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
			<state id="simple"/>
			<cluster><!-- ... --></cluster>
		</ha-jdbc>

*sql*
:	持久的状态管理器,使用内嵌的数据库.
	该提供器支持以下属性,用于管理连接池行为.
	连接池属性的完整列表和默认值可参考 [Apache Commons Pool documentation][commons-pool] 文档.
	<table>
		<tr>
			<th>属性</th>
			<th>默认值</th>
			<th>描述</th>
		</tr>
		<tr>
			<td>**urlPattern**</td>
			<td>
				<div class="nobr">jdbc:h2:{1}/{0}</div><br/>
				<div class="nobr">jdbc:hsqldb:{1}/{0}</div><br/>
				<div class="nobr">jdbc:derby:{1}/{0};create=true</div>
			</td>
			<td>
				A MessageFormat pattern indicating the JDBC url of the embedded database.
				该模式可接受两个参数:
				<ol>
					<li>集群标识符</li>
					<li>`$HOME/.ha-jdbc`</li>
				</ol>
			</td>
		</tr>
		<tr>
			<td>**user**</td>
			<td></td>
			<td>Authentication user name for the embedded database.</td>
		</tr>
		<tr>
			<td>**password**</td>
			<td></td>
			<td>Authentication password for the above user.</td>
		</tr>
	</table>
	例如:

		<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
			<state id="sql">
				<property name="urlPattern">jdbc:h2:/temp/ha-jdbc/{0}</property>
			</state>
			<cluster><!-- ... --></cluster>
		</ha-jdbc>

berkeleydb
:	持久的状态管理器,使用 BerkeleyDB 数据库.
	该提供器支持以下属性,用于管理连接池行为.
	连接池属性的完整列表和默认值可参考 [Apache Commons Pool documentation][commons-pool] 文档.
	<table>
		<tr>
			<th>属性</th>
			<th>默认值</th>
			<th>描述</th>
		</tr>
		<tr>
			<td>**locationPattern**</td>
			<td>
				{1}/{0}
			</td>
			<td>
				A MessageFormat pattern indicating the base location of the embedded database.
				The pattern can accept 2 parameters:
				<ol>
					<li>The cluster identifier</li>
					<li>`$HOME/.ha-jdbc`</li>
				</ol>
			</td>
		</tr>
	</table>
	例如:

		<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
			<state id="berkeleydb">
				<property name="locationPattern">/tmp/{0}</property>
			</state>
			<cluster><!-- ... --></cluster>
		</ha-jdbc>

sqlite
:	持久的状态管理器,使用 SQLite 数据库.
	该提供器支持以下属性,用于管理连接池行为.
	连接池属性的完整列表和默认值可参考 [Apache Commons Pool documentation][commons-pool] 文档.
	<table>
		<tr>
			<th>属性</th>
			<th>默认值</th>
			<th>描述</th>
		</tr>
		<tr>
			<td>**locationPattern**</td>
			<td>
				{1}/{0}
			</td>
			<td>
				A MessageFormat pattern indicating the base location of the embedded database.
				The pattern can accept 2 parameters:
				<ol>
					<li>The cluster identifier</li>
					<li>`$HOME/.ha-jdbc`</li>
				</ol>
			</td>
		</tr>
	</table>
	例如:

		<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
			<state id="sqlite">
				<property name="locationPattern">/tmp/{0}</property>
			</state>
			<cluster><!-- ... --></cluster>
		</ha-jdbc>

###	<a name="durability"/>持久性

在HA-JDBC 3.0版本支持为用户事务配置持久级别.
如果启用了,HA-JDBC 会跟踪事务,因此,是重启或崩溃了,它可以检测并回复部分提交(例如: 一个事务仅在集群的部分节点上完成.)
该持久机制是由状态管理器配置的.
默认情况下,HA-JDBC包含了对以下持久级别的支持:

none
:	不会跟踪调用.
	该持久级别不能检测或恢复提交中崩溃.
	该级别提供了最好的性能,但对于崩溃没有保护.
	只读数据库集群应该使用该级别.

*coarse*
: 	仅跟踪集群调用,而不对每个数据库的调用进行跟踪.
	该持久级别可检测,但不能从提交中崩溃恢复.
	当恢复的时候,如果任何集群调用依然存在在日志中,所有从数据库会被禁用需要被手动从新启用.
	该级别对可靠性和性能做了折中.

fine
:	跟踪集群调用并且跟踪每个数据库调用.
	该持久级别可检测且能从提交中崩溃恢复.
	当恢复时,如果有任何集群调用依然存在在日志中,则只有那些未完成事务的从数据库会被禁用.
	该级别是最慢的,但保证了从崩溃恢复的最高的可靠性.

例如:

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster durability="fine">
			<!-- ... -->
		</cluster>
	</ha-jdbc>



###	<a name="distributed"/>分布能力

指定定义在该文件中的数据库集群是否会被多个JVM访问.
默认情况下,HA-JDBC支持以下提供者:

*jgroups*
:	使用一个 JGroups 管道来广播集群成员的变化到其它节点.
	JGroups识别以下属性:
	<table>
		<tr>
			<th>Property</th>
			<th>Default</th>
			<th>Description</th>
		</tr>
		<tr>
			<td>**stack**</td>
			<td>`udp-sync.xml`</td>
			<td>
				Defines one of the following:
				<ul>
					<li>Name of a system resource containing the JGroups XML configuration.</li>
					<li>URL of the JGroups XML configuration file.</li>
					<li>Path of the JGroups XML configuration on the local file system.</li>
					<li>Legacy protocol stack property string.</li>
				</ul>
				See the [JGroups wiki][jgroups] for assistance with customizing the protocol stack.
			</td>
		</tr>
		<tr>
			<td>**timeout**</td>
			<td>60000</td>
			<td>Indicates the number of milliseconds allowed for JGroups operations.</td>
		</tr>
	</table>

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<distributable id="jgroups">
			<property name="stack">udp.xml</property>
		</distributable>
		<cluster><!-- ... --></cluster>
	</ha-jdbc>


###	<a name="meta-data"/>数据库元数据缓存

HA-JDBC大量使用了数据库的元数据.
出于性能考虑,这些信息应该尽可能的进行缓存.
默认情况下,HA-JDBC 包含了下列元数据缓存(meta-data-cache)选项:

none
:	Meta data is loaded when requested and not cached.

lazy
:	Meta data is loaded and cached per database as it is requested.

none
:	元数据在请求时并且没有缓存时加载.

lazy
:	在对每个数据库进行请求时加载并且缓存元数据.

shared-lazy
:	Meta data is loaded and cached as it is requested.
:	元数据在请求时并且没有缓存时加载.

*eager*
:	All necessary meta data is loaded and cached per database during HA-JDBC initialization.
:	在HA-JDBC初始化的时候,会对每个数据库的元数据进行必要的加载和缓存.

shared-eager
:	All necessary meta data is loaded and cached during HA-JDBC initialization.
:	在HA-JDBC初始化的时候,会对元数据进行必要的加载和缓存.

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster meta-data-cache="shared-eager">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	<a name="password"/>密码混淆

因为HA-JDBC的配置文件包含了数据库密码,可能有的用户会想对密码进行混淆.
使用":"来表示密码启用了混淆机制,并指定相应的解码器.

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster ...>
			<database id="db1" location="jdbc:mysql://server:port/db1">
				<user>admin</user>
				<password>?:wzAkF0hlYUeGhfzRQIxYAQ==</password>
			</database>
			<database id="db2"><!-- ... --></database>
		</cluster>
	</ha-jdbc>

当前支持一下解码机制:

16
:	使用16进制进行解码.

64
:	使用Base64进行解码.

?
:	使用keystore中的key对密码进行解码.
	一下的系统属性可用于自定义key或keystore属性:
	<table>
		<tr>
			<th>系统属性</th>
			<th>默认值</th>
		</tr>
		<tr>
			<td>ha-jdbc.keystore.file</td>
			<td>`$HOME/.keystore`</td>
		</tr>
		<tr>
			<td>ha-jdbc.keystore.type</td>
			<td>jceks</td>
		</tr>
		<tr>
			<td>ha-jdbc.keystore.password</td>
			<td><em>none</em></td>
		</tr>
		<tr>
			<td>ha-jdbc.key.alias</td>
			<td>ha-jdbc</td>
		</tr>
		<tr>
			<td>ha-jdbc.key.password</td>
			<td><em>required</em></td>
		</tr>
	</table>
	使用以下的命令来生成加密后的密码:

		java -classpath ha-jdbc.jar net.sf.hajdbc.codec.crypto.CipherCodecFactory [password]


###	<a name="unique-ids"/>唯一标识符

大多数应用在对数据库写入的时候需要一些主键生成机制.
数据库通常提供两个机制来做这样的事,HA-JDBC对这两种方式都有支持(如果配置的方言<a href="#dialect">dialect</a>支持):数据库序列和标识符(例如:自增列).

需要注意的是,在HA-JDBC使用序列或标识符列的时候可能会有一定的性能影响.
在HA-JDBC中使用这两个算法都会导致对每个语句进行正则匹配和互斥锁的消耗,后者在集群环境中的影响特别明显.
由于他们的性能影响,对于序列或标识符列可以通过集群的 **detect-sequences** 和 **detect-identity-columns** 属性来分别禁用.

例如:

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster detect-sequences="false" detect-identity-columns="false">
			<!-- ... -->
		</cluster>
	</ha-jdbc>

幸运的是,对于序列这样性能的消耗可以通过Hibernate的Sequence-HiLo算法来减缓.

对于最好的性能,HA-JDBC推荐使用基于表的高低或UUID算法,这样便可避免对语句的解析和锁的消耗.
对象关系映射(ORM)框架(例如: Hibernate,OpenJPA 等)通常包含了一些这样算法的实现.

###	定制 HA-JDBC

HA-JDBC支持大量的扩展点.
大多组件都支持自定义实现,包括:

*	[net.sf.hajdbc.balancer.BalancerFactory](apidocs/net/sf/hajdbc/balancer/BalancerFactory.html)
*	[net.sf.hajdbc.cache.DatabaseMetaDataCacheFactory](apidocs/net/sf/hajdbc/cache/DatabaseMetaDataCacheFactory.html)
*	[net.sf.hajdbc.codec.CodecFactory](apidocs/net/sf/hajdbc/codec/CodecFactory.html)
*	[net.sf.hajdbc.dialect.DialectFactory](apidocs/net/sf/hajdbc/dialect/DialectFactory.html)
*	[net.sf.hajdbc.distributed.CommandDispatcherFactory](apidocs/net/sf/hajdbc/distributed/CommandDispatcherFactory.html)
*	[net.sf.hajdbc.durability.DurabilityFactory](apidocs/net/sf/hajdbc/durability/DurabilityFactory.html)
*	[net.sf.hajdbc.lock.LockManagerFactory](apidocs/net/sf/hajdbc/lock/LockManagerFactory.html)
*	[net.sf.hajdbc.state.StateManagerFactory](apidocs/net/sf/hajdbc/state/StateManagerFactory.html)
*	[net.sf.hajdbc.SynchronizationStrategy](apidocs/net/sf/hajdbc/SynchronizationStrategy.html)

通常以如下方式来配置 HA-JDBC 的自定义组件:


1. 创建一个自定义组件的实现.
	所有的扩展点都实现一个 getId() 方法,用于在配置文件中表示该特定实现.
1. 创建一个 `META-INF/services/interface-name` 文件包含了实现类的全限定类名.
1. 通过在你的 ha-jdbc xml 文件中指定标识符来引用自定义的组件.

例如:

	package org.myorg;
	
	public class CustomDialectFactory implements net.sf.hajdbc.dialect.DialectFactory {
		@Override
		public String getId() {
			return "custom";
		}
	
		@Override
		public net.sf.hajdbc.dialect.Dialect createDialect() {
			return new StandardDialect() {
				// 重写方法来自定义行为
			};
		}
	}

`META-INF/services/net.sf.hajdbc.dialect.DialectFactory`

	org.myorg.CustomDialectFactory

`ha-jdbc.xml`

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster dialect="custom" ...>
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	以编程的方式配置

在HA-JDBC 3.0版本后,可通过程序来进行配置.

例如:

	// 定义集群中的数据库
	DriverDatabase db1 = new DriverDatabase();
	db1.setId("db1");
	db1.setLocation("jdbc:hsqldb:mem:db1");
	db1.setUser("sa");
	db1.setPassword("");
	
	DriverDatabase db2 = new DriverDatabase();
	db2.setId("db2");
	db2.setLocation("jdbc:hsqldb:mem:db2");
	db2.setUser("sa");
	db2.setPassword("");
	
	// 定义集群配置
	DriverDatabaseClusterConfiguration config = new DriverDatabaseClusterConfiguration();
	// 指定集群中的数据库
	config.setDatabases(Arrays.asList(db1, db2));
	// 定义方言
	config.setDialectFactory(new HSQLDBDialectFactory());
	// 不缓存元数据
	config.setDatabaseMetaDataCacheFactory(new SimpleDatabaseMetaDataCacheFactory());
	// 使用内存管理器
	config.setStateManagerFactory(new SimpleStateManagerFactory());
	// 使集群可分布
	config.setDispatcherFactory(new JGroupsCommandDispatcherFactory());
	
	// 通过 HA-JDBC 驱动注册配置
	net.sf.hajdbc.sql.Driver.setConfigurationFactory("mycluster", new SimpleDatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase>(config));
	
	// 现在数据库已经可用了
	Connection connection = DriverManager.getConnection("jdbc:ha-jdbc:mycluster", "sa", "");


## 使用 HA-JDBC

可通过 Driver 或 DataSource 来访问 HA-JDBC 集群.


###	基于驱动的访问

基于驱动访问,HA-JDBC 必须相应的配置数据库访问.

例如:

`ha-jdbc-mycluster.xml`

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster>
			<database id="db1" location="jdbc:postgresql://server1:port1/database1">
				<user>pgadmin</user>
				<password>password</password>
			</database>
			<database id="db2" location="jdbc:postgresql://server2:port2/database2">
				<user>pgadmin</user>
				<password>password</password>
			</database>
		</cluster>
	</ha-jdbc>

可以通过制定如下的JDBC URL来建立 HA-JDBC 链接:

	java.sql.Connection connection = java.sql.DriverManager.getConnection("jdbc:ha-jdbc:mycluster", "user", "password");


###	基于数据源的访问

基于数据源访问,HA-JDBC 必须配置为通过数据源连接数据库.
HA-JDBC 提供了 DataSource, ConnectionPoolDataSource 和 XADataSource 实现,依赖你你想代理什么样的资源.

例如:

`ha-jdbc-mycluster.xml`

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster>
			<database id="db1" location="org.postgresql.ds.PGSimpleDataSource">
				<user>pgadmin</user>
				<password>password</password>
				<property name="serverName">server1</property>
				<property name="portNumber">5432</property>
				<property name="databaseName">database</property>
			</database>
			<database id="db2" location="org.postgresql.ds.PGSimpleDataSource">
				<user>pgadmin</user>
				<password>password</password>
				<property name="serverName">server2</property>
				<property name="portNumber">5432</property>
				<property name="databaseName">database</property>
			</database>
		</cluster>
	</ha-jdbc>

例如,在 Tomcat 中部署 HA-JDBC 数据源:

`context.xml`

	<Context>
		<!-- ... -->
		<Resource name="jdbc/mycluster" type="net.sf.hajdbc.sql.DataSource"
		          factory="org.apache.naming.factory.BeanFactory"
		          closeMethod="stop"
		          cluster="mycluster"
		          config="file:///path/to/ha-jdbc-mycluster.xml"/>
		<!-- ... -->
	</Context>

使DataSource在应用中可被引用DataSource:

`web.xml`

	<web-app>
		<!-- ... -->
		<resource-env-ref>
			<resource-env-ref-name>jdbc/mycluster</resource-env-ref-name>
			<resource-env-ref-type>javax.sql.DataSource</resource-env-ref-type>
		</resource-env-ref>
		<!-- ... -->
	</web-app>

可通过以下方式访问集群:

	javax.naming.Context context = new javax.naming.InitialContext();
	javax.sql.DataSource ds = (javax.sql.DataSource) context.lookup("java:comp/env/jdbc/mycluster");
	java.sql.Connection connection = ds.getConnection("user", "password");


###	处理JDBC语句

####	数据库读操作

数据库读操作 (例如: SELECT 语句) 使用以下算法处理:


1.	从<a href="#balancer">均衡器</a>获取下一个数据库.
1.	使用该数据库执行语句.
1.	如果语句执行成功,则返回结果给调用者.
1.	如果执行失败,则解析捕获到的异常.
	1.	如果异常<a href="#failure">认定为失败</a>:
			1.	禁用数据库
			1.	使用下一个有效的数据库重复操作
	1.	如果认定异常 *不是* 失败,则异常被抛回给调用者.

此外,数据库的读操作可配置为同时使用主和备份数据库并发执行.
这样的性能相对更好,但如果多个应用尝试更新同一数据库的行则可能导致死锁.
如果你的场景能接受这样的限制,则可以通过 **transaction-mode** 启用并行写.

例如:

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster transaction-mode="parallel">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


####	数据库写操作

默认情况下,数据库写操作(例如: INSERT/UPDATE/DELETE 语句)使用以下算法处理:

1. 使用主节点执行该语句
1. 如果语句执行失败 *并且* 异常被<a href="#failure">认定为失败</a>:
	1. 禁用该主节点
	1. 使用心得主节点来执行
1. 否则,如果语句执行成功 *或*  异常 *没有* 认定为失败.
	1. 并行的在备份数据库上执行该语句.
	1. 将返回结果与主节点返回的结果比较
	1. 如果结果和主节点返回的结果不同,则禁用该备份节点.
	
###	<a name="failure"/>失败处理

判断在数据库处理时抛出的异常是否是数据库失败,我们会向配置好的方言进行查阅.
默认实现的返回结果如下:

Dialect.indicatesFailure(SQLException)
:	The exception is determined to be a failure if the exception is an instance of `java.sql.SQLNonTransientConnectionException`.
:	如果异常是`java.sql.SQLNonTransientConnectionException`的实例,则认为是数据库失败.

Dialect.indicatesFailure(XAException)
:	如果该异常的错误码是 `XAException.XAER_RMFAIL`,则认为是数据库失败.

方言可以从写这些行为,可能通过添加提供商的代码,SQL状态来判断等.

如果HA-JDBC判断该数据库出现错误,则数据库被禁用.
禁用过程如下:

1.	记录一个错误消息的日志
1.	将数据库从激活的数据库中移除.
1.	如果集群是分布式的(**distributable**),则广播禁用该数据库的消息到其他服务.

数据库可通过<a href="#jmx">JMX</a>手动禁用.

你可以通过配置 **failure-detect-schedule** 属性来使HA-JDBC主动检测数据库.该属性值为一个 cron 的表达式,指定了数据库集群执行该操作的调度时间.

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<!-- 每分钟执行失败检测 -->
		<cluster failure-detect-schedule="0 * * ? * *">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	恢复错误的数据库节点

激活数据库的过程如下:

1.	检测目标数据库是否确实存活.
1.	获取一个锁,阻塞所有客户端对该数据库集群的写操作.
1.	使用给定的同步策略同步目标数据库与主数据库.
1.	将目标数据库添加到激活的数据库集合.
1.	通过<a href="#state">集群状态管理器</a>持久化新的集群状态.
	*	如果数据库集群是分布式的( **distributable** ),广播数据库激活的信息到其他服务节点.
1.	释放在第二步获取的锁.

通常来说,数据库同步是密集型和侵入性的任务.
在管理数据库一致性时,在集群中的每个数据库节点都是读锁(写操作被阻塞)知道同步完成.
因为同步可能在任何地点发生,可能花费几秒钟甚至几小时(依赖于数据库大小和选用的同步策略),如果你的数据库集群是在以读为主的环境中使用,那么推荐在非高峰期执行数据库激活操作.

此外,通过指定 **auto-activate-schedule** 属性,HA-JDBC可尝试自动激活禁用的数据库.
如果指定了, HA-JDBC 会尝试自动激活禁用的数据库,但转变为存活状态,会依据于指定的 cron 调度时间.

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<!-- 每天 2:00 AM 自动激活 -->
		<cluster auto-activate-schedule="0 0 2 ? * *">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	<a name="jmx"/>HA-JDBC 管理

####	数据库集群管理

默认情况下,数据库集群是使用平台的mbean服务使用以下对象名注册的:

net.sf.hajdbc:type=DatabaseCluster,cluster=*cluster-id*

#####	管理属性

id
:	表示该数据库集群的唯一标识符.

active
:	表示该数据库集群是否激活.

version
:	表示使用的 HA-JDBC 版本.

activeDatabases
:	列举当前数据库集群中激活的数据库.

inactiveDatabases
:	列举当前数据库集群中禁用的数据库.

defaultSynchronizationStrategy
:	数据库集群的默认同步策略.

synchronizationStrategies
:	列举该数据库集群可用的同步策略.


#####	Management Operations

isAlive(String databaseId)
:	判断给定的数据库是否有相应是否可以被激活.

activate(String databaseId)
:	使用默认的同步策略激活指定的数据库.

activate(String databaseId, String syncId)
:	使用给定的同步策略激活指定的数据库.

deactivate(String databaseId)
:	禁用指定数据库.

add(String databaseId)
:	使用指定的标识符添加数据库到集群.
	数据库在所有指定的添加完成激活前会保持禁用状态.
	完整的数据库描述使用 net.sf.hajdbc:type=Database,cluster=*cluster-id*,database=*database-id* mbean.
	

remove(String databaseId)
:	Removes the specified database from the cluster.  Only inactive databases may be removed from the cluster.
:	在集群中移除数据库. 只有禁用的数据库可以被移除.

flushMetaDataCache()
:	清空内部的数据库元数据缓存.


> 该文档由 [wener](https://github.com/wenerme) 翻译,如有错误请指正. ;-)

[commons-pool]: http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/GenericObjectPool.html "Apache Commons Pool"
[jgroups]: http://community.jboss.org/wiki/JGroups "JGroups"
