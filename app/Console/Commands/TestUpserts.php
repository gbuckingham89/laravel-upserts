<?php

namespace App\Console\Commands;

use App\Models\Product;
use Faker\Generator;
use Illuminate\Console\Command;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;

/**
 * Class TestUpserts
 *
 * @package App\Console\Commands
 */
class TestUpserts extends Command
{

    /**
     * @var int
     */
    private static int $num_initial_products = 125000;

    /**
     * @var int
     */
    private static int $num_extra_products = 25000;

    /**
     * @var int
     */
    private static int $batch_size = 5000;

    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'test-upserts';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = "Test upsert performance";

    /**
     * @var \Faker\Generator
     */
    private Generator $faker;

    /**
     * @var int
     */
    private int $test_1_time_insert;

    /**
     * @var int
     */
    private int $test_1_time_update;

    /**
     * @var int
     */
    private int $test_2_time_insert;

    /**
     * @var int
     */
    private int $test_2_time_update;

    /**
     * @var int
     */
    private int $test_3_time_insert;

    /**
     * @var int
     */
    private int $test_3_time_update;

    /**
     * @param \Faker\Generator $faker
     */
    public function handle(Generator $faker)
    {
        $this->faker = $faker;

        $this->truncateDb();

        $this->info("Testing with updateOrCreate...");
        $this->testUpdateOrCreate();
        $this->info("Finished testing with updateOrCreate!");

        $this->truncateDb();

        $this->info("Testing with upsert (singular)...");
        $this->testUpsertSingular();
        $this->info("Finished testing with upsert (singular)!");

        $this->truncateDb();

        $this->info("Testing with upsert (batched)...");
        $this->testUpsertBatched();
        $this->info("Finished testing with upsert (batched)!");

        $this->line("=============================");
        $this->line("========== RESULTS ==========");
        $this->line("=============================");

        $this->info("TEST: updateOrCreate");
        $this->line("  INSERT: ".round($this->test_1_time_insert / 1000000, 3)." secs");
        $this->line("  UPDATE: ".round($this->test_1_time_update / 1000000, 3)." secs");

        $this->info("TEST: upsert (singular)");
        $this->line("  INSERT: ".round($this->test_2_time_insert / 1000000, 3)." secs");
        $this->line("  UPDATE: ".round($this->test_2_time_update / 1000000, 3)." secs");

        $this->info("TEST: upsert (batch)");
        $this->line("  INSERT: ".round($this->test_3_time_insert / 1000000, 3)." secs");
        $this->line("  UPDATE: ".round($this->test_3_time_update / 1000000, 3)." secs");
    }

    /**
     *
     */
    private function testUpdateOrCreate()
    {
        $product_data = $this->generateInitialProductData();

        $this->line("Performing INSERT's...");

        $started_at = (int) (microtime(true) * 1000000);

        $product_data->each(function(array $product_data) {
            Product::updateOrCreate(['item_ref' => $product_data['item_ref']], [
                'title'         => $product_data['title'],
                'description'   => $product_data['description'],
                'price'         => $product_data['price'],
            ]);
        });

        $ended_at = (int) (microtime(true) * 1000000);

        $this->line("Finished Performing INSERT's!");

        $this->test_1_time_insert = $ended_at - $started_at;

        $product_data = $this->generateUpdatedProductData($product_data);

        $this->line("Performing UPDATE's...");

        $started_at = (int) (microtime(true) * 1000000);

        $product_data->each(function(array $product_data) {
            Product::updateOrCreate(['item_ref' => $product_data['item_ref']], [
                'title'         => $product_data['title'],
                'description'   => $product_data['description'],
                'price'         => $product_data['price'],
            ]);
        });

        $ended_at = (int) (microtime(true) * 1000000);

        $this->line("Finished Performing UPDATE's!");

        $this->test_1_time_update = $ended_at - $started_at;
    }

    /**
     *
     */
    private function testUpsertSingular()
    {
        $product_data = $this->generateInitialProductData();

        $this->line("Performing INSERT's...");

        $started_at = (int) (microtime(true) * 1000000);

        $product_data->each(function(array $product_data) {
            Product::upsert([$product_data], ['item_ref']);
        });

        $ended_at = (int) (microtime(true) * 1000000);

        $this->line("Finished Performing INSERT's!");

        $this->test_2_time_insert = $ended_at - $started_at;

        $product_data = $this->generateUpdatedProductData($product_data);

        $this->line("Performing UPDATE's...");

        $started_at = (int) (microtime(true) * 1000000);

        $product_data->each(function(array $product_data) {
            Product::upsert([$product_data], ['item_ref']);
        });

        $ended_at = (int) (microtime(true) * 1000000);

        $this->line("Finished Performing UPDATE's!");

        $this->test_2_time_update = $ended_at - $started_at;
    }

    /**
     *
     */
    private function testUpsertBatched()
    {
        $product_data = $this->generateInitialProductData();

        $this->line("Performing INSERT's...");

        $started_at = (int) (microtime(true) * 1000000);

        $product_data->chunk(self::$batch_size)->each(function(Collection $batch) {
            Product::upsert($batch->toArray(), ['item_ref']);
        });

        $ended_at = (int) (microtime(true) * 1000000);

        $this->line("Finished Performing INSERT's!");

        $this->test_3_time_insert = $ended_at - $started_at;

        $product_data = $this->generateUpdatedProductData($product_data);

        $this->line("Performing UPDATE's...");

        $started_at = (int) (microtime(true) * 1000000);

        $product_data->chunk(self::$batch_size)->each(function(Collection $batch) {
            Product::upsert($batch->toArray(), ['item_ref']);
        });

        $ended_at = (int) (microtime(true) * 1000000);

        $this->line("Finished Performing UPDATE's!");

        $this->test_3_time_update = $ended_at - $started_at;
    }

    /**
     *
     */
    private function truncateDb()
    {
        $this->info("Truncating database table...");

        DB::table('products')->truncate();
    }

    /**
     * @return \Illuminate\Support\Collection
     */
    private function generateInitialProductData(): Collection
    {
        $this->line("Generating product data...");

        $product_data = new Collection();

        for($i=1; $i<=self::$num_initial_products; $i++)
        {
            $product_data->push([
                'item_ref'      => str_pad($i, 8, '0', STR_PAD_LEFT),
                'title'         => "Large birthday cake",
                'description'   => "A large, but extremely tasty chocolate birthday cake",
                'price'         => $this->faker->numberBetween(0, 50000),
            ]);
        }

        $this->line("Finished generating product data...");

        return $product_data->shuffle();
    }

    /**
     * @param \Illuminate\Support\Collection $product_data
     *
     * @return \Illuminate\Support\Collection
     */
    private function generateUpdatedProductData(Collection $product_data)
    {
        $this->line("Generating updated product data...");

        $product_data = $product_data->map(function(array $product_data) {
            $product_data['price'] = $this->faker->numberBetween(0, 50000);
            return $product_data;
        });

        for($i=(self::$num_initial_products + 1); $i<=(self::$num_initial_products + self::$num_extra_products); $i++)
        {
            $product_data->push([
                'item_ref'      => str_pad($i, 8, '0', STR_PAD_LEFT),
                'title'         => "Large birthday cake",
                'description'   => "A large, but extremely tasty chocolate birthday cake",
                'price'         => $this->faker->numberBetween(0, 50000),
            ]);
        }

        $this->line("Finished generating updated product data...");

        return $product_data->shuffle();
    }

}
