import { load_config } from '../src/config/app_config.js';
import { create_logger } from '../src/config/logger.js';
import { connect_mongo, ensure_indexes } from '../src/db/mongo.js';

const config = load_config();
const logger = create_logger(config.log_level);
const mongo = await connect_mongo(config, logger);

try {
  await ensure_indexes(mongo.db);
  logger.info('indexes_ready');
} finally {
  await mongo.close();
}
