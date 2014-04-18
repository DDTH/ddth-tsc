package com.github.ddth.tsc.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.github.ddth.tsc.AbstractCounterFactory;
import com.github.ddth.tsc.ICounter;
import com.github.ddth.tsc.cassandra.internal.CounterMetadata;
import com.github.ddth.tsc.cassandra.internal.MetadataManager;

/**
 * This factory creates {@link CassandraCounter} instances.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.2.0
 */
public class CassandraCounterFactory extends AbstractCounterFactory {

	private final static String[] EMPTY_STRING_ARR = new String[0];

	private final Logger LOGGER = LoggerFactory
			.getLogger(CassandraCounterFactory.class);

	private List<String> hosts = new ArrayList<String>();
	private String keyspace,
			tableMetadata = MetadataManager.DEFAULT_METADATA_TABLE;
	private int port = 9042;
	private boolean myOwnCluster = false;
	private Cluster cluster;
	private Session session;

	private MetadataManager metadataManager;

	public CassandraCounterFactory addHost(String host) {
		hosts.add(host);
		return this;
	}

	public String getHost() {
		return hosts.size() > 0 ? hosts.get(0) : null;
	}

	public CassandraCounterFactory setHost(String host) {
		hosts.clear();
		hosts.add(host);
		return this;
	}

	public Collection<String> getHosts() {
		return this.hosts;
	}

	public CassandraCounterFactory setHosts(Collection<String> hosts) {
		this.hosts.clear();
		if (hosts != null) {
			this.hosts.addAll(hosts);
		}
		return this;
	}

	public CassandraCounterFactory setHosts(String[] hosts) {
		this.hosts.clear();
		if (hosts != null) {
			for (String host : hosts) {
				this.hosts.add(host);
			}
		}
		return this;
	}

	public int getPort() {
		return port;
	}

	public CassandraCounterFactory setPort(int port) {
		this.port = port;
		return this;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public CassandraCounterFactory setKeyspace(String keyspace) {
		this.keyspace = keyspace;
		return this;
	}

	public String getTableMetadata() {
		return tableMetadata;
	}

	public CassandraCounterFactory setTableMetadata(String tableMetadata) {
		this.tableMetadata = tableMetadata;
		return this;
	}

	public CassandraCounterFactory setCluster(Cluster cluster) {
		if (session != null) {
			session.close();
			session = null;
		}
		if (this.cluster != null && myOwnCluster) {
			this.cluster.close();
		}
		this.cluster = cluster;
		myOwnCluster = false;

		return this;
	}

	public Cluster getCluster() {
		return cluster;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CassandraCounterFactory init() {
		if (cluster == null) {
			cluster = Cluster.builder()
					.addContactPoints(hosts.toArray(EMPTY_STRING_ARR))
					.withPort(port).build();
			myOwnCluster = true;
		}
		session = cluster.connect(keyspace);

		metadataManager = new MetadataManager();
		metadataManager.setCluster(cluster).setKeyspace(keyspace)
				.setTableMetadata(tableMetadata);
		metadataManager.init();

		return (CassandraCounterFactory) super.init();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void destroy() {
		try {
			super.destroy();
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
		}

		if (metadataManager != null) {
			try {
				metadataManager.destroy();
			} catch (Exception e) {
				LOGGER.warn(e.getMessage(), e);
			} finally {
				metadataManager = null;
			}
		}

		if (session != null) {
			try {
				session.close();
			} catch (Exception e) {
				LOGGER.warn(e.getMessage(), e);
			} finally {
				session = null;
			}
		}

		if (cluster != null && myOwnCluster) {
			try {
				cluster.close();
			} catch (Exception e) {
				LOGGER.warn(e.getMessage(), e);
			} finally {
				cluster = null;
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected ICounter createCounter(String name) {
		CounterMetadata metadata = metadataManager.getCounterMetadata(name);
		if (metadata == null) {
			throw new IllegalStateException("No metadata found for counter ["
					+ name + "]!");
		}

		CassandraCounter counter = new CassandraCounter();
		counter.setName(name);
		counter.setSession(session);
		counter.setMetadata(metadata);
		counter.init();
		return counter;
	}

}
