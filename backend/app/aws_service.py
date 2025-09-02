import boto3
import gzip
import io
import json
import numpy as np
import os
import pandas as pd
import pickle
import struct
import zipfile
from botocore.exceptions import ClientError
from PIL import Image

from app.utils.config import get_config


class AWSService:
    def __init__(self):
        aws_config = get_config().get("aws")
        if not aws_config:
            raise ValueError("AWS configuration is missing in the config file")

        aws_access_key_id = aws_config.get("access_key_id")
        aws_secret_access_key = aws_config.get("secret_access_key")
        region_name = aws_config.get("region")
        bucket_name = aws_config.get("bucket_name")
        self.env = aws_config.get("env")
        if not all(
            [aws_access_key_id, aws_secret_access_key, region_name, bucket_name]
        ):
            raise ValueError("Incomplete AWS configuration in the config file")

        self.s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        self.bucket = self.s3_resource.Bucket(bucket_name)

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        self.ai_bucket_name = aws_config.get("ai_bucket_name")
        self.ai_bucket = self.s3_resource.Bucket(self.ai_bucket_name)

    def download_yolo_weights_from_s3(self, yolo_directory, local_file_path):
        version_text = os.path.join(yolo_directory, "latest_version.txt")
        version_buffer = io.BytesIO()
        self.s3_client.download_fileobj(
            self.ai_bucket_name, version_text, version_buffer
        )
        version_buffer.seek(0)
        latest_version = version_buffer.read().decode("utf-8").strip()
        latest_yolo_folder = os.path.join(yolo_directory, latest_version)
        latest_yolo = os.path.join(latest_yolo_folder, "model.pt")
        self.s3_client.download_file(self.ai_bucket_name, latest_yolo, local_file_path)

    def read_image_from_s3(self, key, bucket=None):
        bucket = self.bucket if bucket is None else bucket
        image = bucket.Object(key)
        img_data = image.get().get("Body").read()
        return np.array(Image.open(io.BytesIO(img_data)))[..., :3].astype(np.uint8)

    def _object_exists(self, bucket_name: str, key: str) -> bool:
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError:
            return False

    def _ensure_dir_key(self, key: str) -> str:
        # avoid accidental '//' in keys
        while "//" in key:
            key = key.replace("//", "/")
        if key.startswith("/"):
            key = key[1:]
        return key

    def get_optimized_presigned_url(
        self,
        key: str,
        *,
        bucket_name: str | None = None,
        target_width: int = 1280,
        image_format: str = "WEBP",
        quality: int = 75,
        cache_max_age_seconds: int = 7 * 24 * 3600,
        presign_seconds: int = 3600,
    ) -> str:
        """
        Create (if needed) a web-optimized rendition for the given S3 object and return a presigned URL.
        If key is an http(s) URL, returns it as-is.
        """
        try:
            if key.startswith("http://") or key.startswith("https://"):
                return key

            # Resolve bucket
            resolved_bucket = bucket_name or self.bucket.name

            # Optimized key path under same bucket
            optimized_key = self._ensure_dir_key(f"optimized/w{target_width}/{key}")

            # If exists, return presigned
            if self._object_exists(resolved_bucket, optimized_key):
                return self.generate_presigned_url(
                    optimized_key,
                    expires_in_seconds=presign_seconds,
                    bucket_name=resolved_bucket,
                )

            # Otherwise, read, resize, and upload
            # Read original bytes
            obj = self.s3_client.get_object(Bucket=resolved_bucket, Key=key)
            original_bytes = obj["Body"].read()
            pil_img = Image.open(io.BytesIO(original_bytes)).convert("RGB")
            # Compute new size keeping aspect ratio
            width, height = pil_img.size
            if width > target_width:
                new_height = int(height * (target_width / float(width)))
                pil_img = pil_img.resize((target_width, new_height), Image.LANCZOS)
            # Encode
            buf = io.BytesIO()
            fmt = "WEBP" if image_format.upper() == "WEBP" else "JPEG"
            content_type = "image/webp" if fmt == "WEBP" else "image/jpeg"
            if fmt == "WEBP":
                pil_img.save(buf, format=fmt, quality=quality, method=6)
            else:
                pil_img.save(buf, format=fmt, quality=quality, optimize=True)
            buf.seek(0)
            # Upload optimized object
            self.s3_client.put_object(
                Bucket=resolved_bucket,
                Key=optimized_key,
                Body=buf.getvalue(),
                ContentType=content_type,
                CacheControl=f"public, max-age={cache_max_age_seconds}",
            )
            # Presign
            return self.generate_presigned_url(
                optimized_key,
                expires_in_seconds=presign_seconds,
                bucket_name=resolved_bucket,
            )
        except Exception as e:
            print(f"Failed to optimize image {key}: {e}")
            # Fallback to standard presign
            return self.generate_presigned_url(
                key, expires_in_seconds=presign_seconds, bucket_name=bucket_name
            )

    def generate_presigned_url(
        self,
        key: str,
        expires_in_seconds: int = 3600,
        use_ai_bucket: bool = False,
        bucket_name: str | None = None,
    ) -> str:
        try:
            bucket_name = bucket_name or (
                self.ai_bucket_name if use_ai_bucket else self.bucket.name
            )
            return self.s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": key},
                ExpiresIn=expires_in_seconds,
            )
        except Exception as e:
            print(f"Failed to create presigned url for {key}: {e}")
            return ""

    def get_pan_embeddings(self, pickle_dir):
        try:
            embedding_pickle = self.ai_bucket.Object(pickle_dir)
            embedding_data = embedding_pickle.get().get("Body").read()
            return pickle.load(io.BytesIO(embedding_data))

        except:
            print("No Embeddings Found")
            return {}

    def save_embedding_to_s3(self, embeddings, key):
        try:
            pickle_data = pickle.dumps(embeddings)
            self.ai_bucket.put_object(Key=key, Body=pickle_data)
            # print(f"Dictionary saved as pickle to s3://{self.ai_bucket_name}/{key}")
        except Exception as e:
            print(f"Error saving dictionary as pickle: {e}")

    def upload_image_to_s3(self, image_array, object_key):
        image = Image.fromarray(image_array)
        buffer = io.BytesIO()
        image.save(buffer, format="JPEG")
        buffer.seek(0)
        try:
            self.s3_client.upload_fileobj(buffer, self.ai_bucket_name, object_key)
            # print(f"Image successfully uploaded to s3://{self.ai_bucket_name}/{object_key}")
        except Exception as e:
            print(f"Error saving Image: {e}")

    def list_s3_objects(self, bucket_name, prefix):
        return [
            obj["Key"]
            for obj in self.s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix
            ).get("Contents", [])
        ]

    def get_existing_pan_dimensions(self, resturaunt_id):
        remove_prefix = lambda larger_string, prefix_to_remove: (
            larger_string[len(prefix_to_remove) :]
            if larger_string.startswith(prefix_to_remove)
            else larger_string
        )
        print(f"getting get_existing_pan_dimensions")
        folders_name = set()
        resturaunt_prefix = f"pan_designs/{self.env}/{resturaunt_id}/"
        # print(f'resturaunt_prefix = {resturaunt_prefix}')
        prefixes = self.list_s3_objects(self.ai_bucket_name, resturaunt_prefix)

        for prefix in prefixes:
            # print(f'current prefix = {prefix}')
            removed_prefix = remove_prefix(prefix, resturaunt_prefix)
            if "/" not in removed_prefix:
                continue
            folder_name = removed_prefix.split("/")[0]
            folders_name.add(folder_name)
        return list(folders_name)

    def get_depth_array_from_s3(self, key, binary_depth_image=False):
        object = self.bucket.Object(key)
        # get object metadata
        try:
            metadata = object.get().get("Metadata")
            width = int(metadata.get("width", 1280))
            height = int(metadata.get("height", 720))
        except:
            pass
        gzipped_depth = object.get().get("Body").read()
        decompressed_depth = gzip.decompress(gzipped_depth)
        if binary_depth_image is True:
            depth = (
                np.frombuffer(decompressed_depth, dtype=np.float32).reshape(
                    (height, width)
                )
                * 1000
            )
        else:
            depth_nested_list = json.loads(decompressed_depth.decode("utf-8"))
            depth_2d = np.array(depth_nested_list)
            depth = np.asanyarray(depth_2d).astype(np.float32) * 1000
        return depth

    def upload_food_embedding_meta_data_to_s3(self, prefix, metadata_buffer):
        try:
            self.ai_bucket.put_object(Key=prefix, Body=metadata_buffer)
        except Exception as e:
            print(f"Error saving food metadata as: {e}")

    def get_latest_food_embedding_metadata(self, restraurant_id, venue, version=None):
        if not version:
            version = self.get_latest_food_embedding_version(restraurant_id, venue)
        try:
            if version:
                base_metadata_prefix = f"metron/verified_embeddings/{restraurant_id}/{venue}/meta_data-{venue}-{restraurant_id}-{version}"
                extensions = [".csv", ".parquet"]
                for ext in extensions:
                    metadata_prefix = base_metadata_prefix + ext
                    try:
                        response = self.ai_bucket.Object(metadata_prefix).get()
                        metadata = response["Body"].read()
                        metadata_buffer = io.BytesIO(metadata)
                        # Read the file based on the extension
                        if ext == ".csv":
                            return pd.read_csv(metadata_buffer)
                        elif ext == ".parquet":
                            return pd.read_parquet(metadata_buffer)
                    except Exception as e:
                        print(
                            f"Failed to read file: {metadata_prefix}. Trying next option..."
                        )
                print(
                    f"No valid metadata file found for Restaurant: {restraurant_id}, Venue: {venue}, Version: {version}"
                )
                return None
            else:
                return None
        except Exception as e:
            print(
                f"Failed to retrieve latest metadata for Restaurants: {restraurant_id} Venue: {venue}"
            )
            print(f"Failed with following excpetion: {e}")
            return None

    def update_latest_food_embedding_version(self, restraurant_id, venue, version):
        try:
            prefix = f"metron/verified_embeddings/{restraurant_id}/{venue}/latest_embedding_version.txt"
            self.ai_bucket.put_object(Key=prefix, Body=version)
        except Exception as e:
            print(f"Latest Embedding version not updated")

    def get_latest_food_embedding_version(self, restaurant_id, venue):
        try:
            prefix = f"{self.env}/food_classification/{restaurant_id}/{venue}/latest_embedding_version.txt"
            s3_object = self.ai_bucket.Object(prefix)
            response = s3_object.get()
            version = response["Body"].read().decode("utf-8").strip()
            if version:
                return version
            return None
        except Exception as e:
            return None

    def get_latest_food_embedding(self, restaurant_id, venue):
        version = self.get_latest_food_embedding_version(restaurant_id, venue)
        if version:
            try:
                embedding_path = f"{self.env}/food_classification/{restaurant_id}/{venue}/verified-embedding-{venue}-{restaurant_id}-{version}.pkl"
                venue_embedding = self.ai_bucket.Object(embedding_path)
                response = venue_embedding.get()
                embedding_data = response["Body"].read()
                return pickle.load(io.BytesIO(embedding_data))
            except Exception as e:
                print(f"Failed to get latest food embedding: {e}")
                return {}
        return {}

    def search_for_food_embeddings_across_venues(self, restaurant_id, venue):
        restaurant_s3_path = f"metron/verified_embeddings/{restaurant_id}/"
        directories_in_restaurant = self.s3_client.list_objects_v2(
            Bucket=self.ai_bucket_name, Prefix=restaurant_s3_path, Delimiter="/"
        )
        other_venues_in_restaurant = [
            cp["Prefix"].rstrip("/").split("/")[-1]
            for cp in directories_in_restaurant.get("CommonPrefixes", [])
            if cp["Prefix"].rstrip("/").split("/")[-1] != venue
        ]
        return other_venues_in_restaurant

    def upload_venue_specific_food_classification_unittest(
        self, unittest_prefix, unittest_buffer
    ):
        try:
            self.ai_bucket.put_object(Key=unittest_prefix, Body=unittest_buffer)
        except Exception as e:
            print(f"Failed to upload unittest folder with exception: {e}")

    def get_venue_specific_food_classification_unittest(
        self, restaurant_id, venue, save_path
    ):
        print(f"Downloading unittest")
        try:
            s3_path = f"metron/unit_test_data/food_classification/{restaurant_id}/{venue}/menu_items_test_data.zip"
            unittest_buffer = io.BytesIO()
            self.ai_bucket.download_fileobj(s3_path, unittest_buffer)
            unittest_buffer.seek(0)
            with zipfile.ZipFile(unittest_buffer, "r") as zip_ref:
                zip_ref.extractall(save_path)
        except Exception as e:
            print(f" Failed to Download Unittest Data with Exception: {e}")

    def upload_file_and_generate_url(self, file_buffer, object_key, expiration=604800):
        try:
            self.ai_bucket.put_object(Key=object_key, Body=file_buffer)
            print(
                f"File successfully uploaded to s3://{self.ai_bucket_name}/{object_key}"
            )

            # Generate a pre-signed URL with maximum expiration time (7 days)
            presigned_url = self.s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.ai_bucket_name, "Key": object_key},
                ExpiresIn=expiration,
            )
            return presigned_url

        except Exception as e:
            print(f"Failed to upload file or generate URL with exception: {e}")
            return "None"
